package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
	"github.com/astaxie/beego/orm"
	"github.com/ydWz/common"
	"github.com/ydWz/models"
)

// func init() {
// 	fmt.Println("pgWalletTransactionsTask")
// 	if beego.BConfig.Listen.HTTPPort != 8083 {
// 		logs.Info("8081、8082端口不启动定时任务")
// 		return
// 	}
// 	pgWalletTransactionsTask()
// 	//pgDailyTask()
// }

func pgWalletTransactionsTask() {
	ticker := time.NewTicker(time.Minute)
	go func() {
		for true {
			select {
			case <-ticker.C:
				PgGetWalletHistory()
			}
		}
		ticker.Stop()
	}()
}

func PgGetWalletHistory() {
	logs.Info("--------------------PgGetWalletHistory--------------------")
	defer func() {
		if err := recover(); err != nil {
			logs.Error("PgGetHistory panic:", err)
		}
	}()

	infos, err := models.GetGamePlatformUpdateByManufacturer("PG_WALLET")
	if err != nil {
		logs.Error("GetGamePlatformUpdateByManufacturer error:", err.Error())
		return
	}
	if len(infos) == 0 {
		return
	}
	for _, info := range infos {
		tableNo := info.TableNo
		var lastId int
		if len(info.UpdateTimeStr) > 0 {
			if lastId, err = strconv.Atoi(info.UpdateTimeStr); err != nil {
				logs.Error("PgGetHistory id string parse int error: "+info.UpdateTimeStr, err.Error())
				return
			}
		}

		//var turnovers []*models.PlayerTurnoverNew
		endTime := time.Now().Unix() - 15*60
		var timeNowDate = time.Now().Unix()
		var rows int
		rows, lastId, err = pgBet2TurnoverNew(tableNo, lastId)
		//if len(turnovers) == 0 {
		//	return
		//}
		logs.Info("PG拉取流水完成, 开始时间：%v, 结束时间：%v", timeNowDate, endTime)
		//logs.Info("获取到的数据总数量：%v", len(turnovers))
		//go func() {
		//	BatchUpdatePlayerTurnover(turnovers)
		//	BatchSaveOrUpdatePlayerTurnover(turnovers)
		//	logs.Info("PG拉取流水完成, 开始时间：%v, 结束时间：%v", timeNowDate, endTime)
		//}()

		if rows == 0 {
			var model = orm.NewOrm()
			//切换拉流水数据源
			if err = model.Using(models.DbWallet); err != nil {
				return
			}
			table, err := models.GetTable(beego.BConfig.AppName)
			if err != nil {
				logs.Error("pg_seamless_transaction_task models.GetTable error:", err.Error())
			} else {
				config, err := models.GetPlatformConfig("PG")
				if err != nil {
					logs.Error("pg_seamless_transaction_task models.GetPgPlatformConfig error:", err.Error())
				} else {
					if lastId >= models.DBTotalMax && table.Sequence-1 > tableNo && config.IsSeamless == 1 && config.IsMaintain == 0 {
						info.UpdateTime = int64(lastId)
						tableNo += 1
						lastId = 0
					}
				}
			}
		}

		info.TableNo = tableNo
		info.UpdateTimeStr = strconv.Itoa(lastId)
		err = models.UpdateGamePlatformUpdateById(info)
		if err != nil {
			logs.Error("UpdateGamePlatformUpdateById error:", err.Error())
		}
		logs.Info("PG拉取流水结束时间:", endTime)
	}
}

func pgBet2TurnoverNew(tableNo, betId int) (int, int, error) {
	//读取单一钱包配置
	//table, err2 := models.GetTable(beego.BConfig.AppName)
	//if err2 != nil {
	//	logs.Error("pgBet2TurnoverNew GetTable error:", err2.Error())
	//	return []*models.PlayerTurnoverNew{}, betId, nil
	//}

	var model = orm.NewOrm()
	//切换拉流水数据源
	if err := model.Using(beego.BConfig.AppName); err != nil {
		return -1, betId, err
	}

	var result []*models.PgBet
	var sql = "SELECT id, player_name,game_id,bet_amount,win_amount,transfer_amount,tax FROM %s WHERE id > ? LIMIT 20000"
	logs.Info("获取sql=", sql)

	serial := fmt.Sprintf("%04d", tableNo)
	tableName := fmt.Sprintf("%s%s", "bet_", serial)
	sql = fmt.Sprintf(sql, tableName)
	var rows, err = model.Raw(sql, betId).QueryRows(&result)
	logs.Info("PgBet2TurnoverNew response， rows:%v, err:%v", rows, err)

	if err != nil {
		return -1, betId, err
	}

	marshal, err := json.Marshal(result)
	logs.Debug("PgBet2TurnoverNew response data:%v, error:%v", string(marshal), err)

	if len(result) == 0 {
		return 0, betId, nil
	}
	if err = model.Using(models.DbApp); err != nil {
		return -1, betId, err
	}
	var userIdsMap = make(map[string]struct{})
	var gameIdsMap = make(map[string]struct{})
	var lastId int
	for _, i2 := range result {
		split1 := strings.Split(i2.PlayerId, "_")

		playerId := "0"
		if len(split1) == 1 {
			playerId = i2.PlayerId
		} else {
			playerId = split1[1]
		}

		userIdsMap[playerId] = struct{}{}
		gameIdsMap[strconv.Itoa(i2.GameId)] = struct{}{}

		if i2.Id > lastId {
			lastId = i2.Id
		}
	}

	var userids = make([]string, 0, len(userIdsMap))
	var gameids = make([]string, 0, len(gameIdsMap))
	for id, _ := range userIdsMap {
		userids = append(userids, id)
	}
	for id, _ := range gameIdsMap {
		gameids = append(gameids, id)
	}

	if len(userids) == 0 || len(gameids) == 0 {
		return -1, lastId, errors.New("用户信息或游戏信息未获取到")
	}
	var whereIn1 = strings.Repeat("?,", len(userids))
	whereIn1 = whereIn1[:len(whereIn1)-1]
	var whereIn2 = strings.Repeat("?,", len(gameids))
	whereIn2 = whereIn2[:len(whereIn2)-1]
	var userinfo []*models.Player
	var gameinfo []*models.GameConfig
	sql = fmt.Sprintf("SELECT * FROM player WHERE id IN (%s)", whereIn1)
	_, err = model.Raw(sql, userids).QueryRows(&userinfo)
	if err != nil {
		return -1, lastId, err
	}

	sql = fmt.Sprintf("SELECT id, type,`name`, gameType,gameCode FROM game_config WHERE gameCode IN (%s) and manufacturer=?", whereIn2)
	_, err = model.Raw(sql, gameids, "PG").QueryRows(&gameinfo)
	if err != nil {
		return -1, lastId, err
	}

	var userinfomap = make(map[string]*models.Player)
	var gameinfomap = make(map[string]*models.GameConfig)
	for _, i2 := range userinfo {
		userinfomap[strconv.Itoa(int(i2.Id))] = i2
	}
	for _, i2 := range gameinfo {
		gameinfomap[i2.GameCode] = i2
	}

	//每次循环置空
	var result1000 []*models.PgBet
	var turnovers []*models.PlayerTurnoverNew
	for i, v := range result {
		result1000 = append(result1000, v)
		//判定整除执行
		if i != 0 && i%1000 == 0 {
			for _, bet := range result1000 {
				split1 := strings.Split(bet.PlayerId, "_")

				playerId := "0"
				if len(split1) == 1 {
					playerId = bet.PlayerId
				} else {
					playerId = split1[1]
				}

				var user, ok1 = userinfomap[playerId]
				var game, ok2 = gameinfomap[strconv.Itoa(bet.GameId)]
				if false == ok1 || false == ok2 {
					logs.Error("未找到数据游戏ID=", bet.GameId, ",用户ID=", bet.PlayerId)
					continue
				}
				turnovers = append(turnovers, &models.PlayerTurnoverNew{
					PlayerId:     user.Id,
					PlayerName:   user.PlayerName,
					Manufacturer: "PG",
					Type:         game.Type,
					Name:         game.Name,
					BetAmount:    float64(bet.BetAmount) / 100,
					ActualBet:    float64(bet.BetAmount) / 100,
					WinAmount:    float64(bet.WinAmount) / 100,
					Profit:       float64(bet.BetAmount-bet.WinAmount) / 100,
					AppId:        user.AppId,
					KfId:         user.KfId,
					CreateTime:   common.GetTimeByTodayZero(),
					SuperiorId:   user.SuperiorId,
					GrandId:      user.GrandId,
				})
				//}()
			}
			logs.Info("获取到的数据总数量：%v", len(turnovers))
			//go func() {
			BatchUpdatePlayerTurnover(turnovers)
			BatchSaveOrUpdatePlayerTurnover(turnovers)
			//执行完置空
			logs.Info("执行PG流水1000条完成,清空数据.当前循环 i=", i+1)
			turnovers = nil
			result1000 = nil
		}
	}
	//判定是否存在剩余的数据单独执行
	if result1000 != nil {
		for _, bet := range result1000 {
			split1 := strings.Split(bet.PlayerId, "_")

			playerId := "0"
			if len(split1) == 1 {
				playerId = bet.PlayerId
			} else {
				playerId = split1[1]
			}

			var user, ok1 = userinfomap[playerId]
			var game, ok2 = gameinfomap[strconv.Itoa(bet.GameId)]
			if false == ok1 || false == ok2 {
				logs.Error("未找到数据游戏ID=", bet.GameId, ",用户ID=", bet.PlayerId)
				continue
			}
			turnovers = append(turnovers, &models.PlayerTurnoverNew{
				PlayerId:     user.Id,
				PlayerName:   user.PlayerName,
				Manufacturer: "PG",
				Type:         game.Type,
				Name:         game.Name,
				BetAmount:    float64(bet.BetAmount) / 100,
				ActualBet:    float64(bet.BetAmount) / 100,
				WinAmount:    float64(bet.WinAmount) / 100,
				Profit:       float64(bet.BetAmount-bet.WinAmount) / 100,
				AppId:        user.AppId,
				KfId:         user.KfId,
				CreateTime:   common.GetTimeByTodayZero(),
				SuperiorId:   user.SuperiorId,
				GrandId:      user.GrandId,
			})

		}
		logs.Info("获取到的数据总数量：%v", len(turnovers))
		//go func() {
		BatchUpdatePlayerTurnover(turnovers)
		BatchSaveOrUpdatePlayerTurnover(turnovers)
	}
	return -1, lastId, nil
}

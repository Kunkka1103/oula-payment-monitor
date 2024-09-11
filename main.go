package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

var (
	dsn         string
	robot1URL   string
	robot2URL   string
	mentions    string
	checkTime   string
	interval    time.Duration
	alertInterval time.Duration
)

type DingTalkMessage struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
	At struct {
		AtMobiles []string `json:"atMobiles"`
		IsAtAll   bool     `json:"isAtAll"`
	} `json:"at"`
}

func init() {
	flag.StringVar(&dsn, "dsn", "postgres://user:password@localhost/dbname?sslmode=disable", "PostgreSQL DSN")
	flag.StringVar(&robot1URL, "robot1", "", "钉钉机器人1的URL")
	flag.StringVar(&robot2URL, "robot2", "", "钉钉机器人2的URL")
	flag.StringVar(&mentions, "mentions", "", "需要@的钉钉用户，使用逗号分隔")
	flag.StringVar(&checkTime, "checkTime", "12:00", "每天开始监控的时间 (格式: HH:MM)")
	flag.DurationVar(&interval, "interval", 5*time.Minute, "首次检查后每次查询的间隔时间")
	flag.DurationVar(&alertInterval, "alertInterval", 30*time.Minute, "如果查询结果为0，告警的间隔时间")
}

func main() {
	flag.Parse()

	// 连接数据库
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("连接数据库失败：", err)
	}
	defer db.Close()

	// 每天定时任务
	for {
		now := time.Now()
		nextCheckTime, err := time.ParseInLocation("15:04", checkTime, now.Location())
		if err != nil {
			log.Fatal("解析检查时间失败：", err)
		}

		// 如果下次检查时间已经过去，则设定为明天的同一时间
		if nextCheckTime.Before(now) {
			nextCheckTime = nextCheckTime.Add(24 * time.Hour)
		}

		// 等待到达下次检查时间
		time.Sleep(time.Until(nextCheckTime))

		// 开始首次监控并定期检查
		checkAndAlert(db)

		// 继续每5分钟检查一次
		ticker := time.NewTicker(interval)
		for range ticker.C {
			checkAndAlert(db)
		}
	}
}

func checkAndAlert(db *sql.DB) {
	// 执行查询
	var count int
	query := `
		SELECT count(*)
		FROM public.distributor
		WHERE pay_status = 'done'
		AND DATE(distributor_date) = CURRENT_DATE - INTERVAL '1 day';
	`
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Println("执行查询时出错：", err)
		return
	}

	// 如果count为0，每半个小时发送告警，直到查询结果改变
	if count == 0 {
		sendAlert("今日打款未完成，请尽快处理", true)
		ticker := time.NewTicker(alertInterval)
		for range ticker.C {
			err = db.QueryRow(query).Scan(&count)
			if err != nil || count != 0 {
				ticker.Stop()
				sendAlert("今日打款已完成", false)
				return
			}
			sendAlert("今日打款未完成，请尽快处理", true)
		}
	} else {
		sendAlert("今日打款已完成", false)
	}
}

func sendAlert(message string, needMentions bool) {
	sendToRobot(robot1URL, message, needMentions)
	sendToRobot(robot2URL, message, false) // 第二个机器人不需要@人
}

func sendToRobot(url, message string, needMentions bool) {
	msg := DingTalkMessage{
		MsgType: "text",
	}
	msg.Text.Content = message

	if needMentions && mentions != "" {
		msg.At.AtMobiles = append(msg.At.AtMobiles, mentions)
		msg.At.IsAtAll = false
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		log.Println("序列化消息时出错：", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Println("发送告警时出错：", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Println("钉钉响应非200状态：", resp.Status)
	}
}

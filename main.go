package main

import (
	"database/sql"
	"flag"
	"log"
	"strings"
	"time"
	"net/http"
	"bytes"
	"encoding/json"

	_ "github.com/lib/pq"
)

var (
	dsn           string
	robot1URL     string
	robot2URL     string
	mentions      string
	checkTime     string
	interval      time.Duration
	alertInterval time.Duration
	isCompleted   bool // 标记是否当天已完成打款
	alertSent     bool // 标记是否已发送过告警
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
	flag.DurationVar(&interval, "interval", 5*time.Minute, "打款未完成时每次检查的间隔时间")
	flag.DurationVar(&alertInterval, "alertInterval", 30*time.Minute, "打款未完成时告警的间隔时间")
}

func main() {
	flag.Parse()

	// 连接数据库
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("连接数据库失败：", err)
	}
	defer db.Close()

	log.Println("启动定时监控任务...")

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

		log.Printf("下次检查时间为：%s", nextCheckTime)

		// 等待到达下次检查时间
		time.Sleep(time.Until(nextCheckTime))

		// 初始化状态
		isCompleted = false
		alertSent = false
		log.Println("初始化状态，开始今日的监控任务")

		// 开始首次监控并定期检查
		checkAndAlert(db)

		// 如果未完成，继续每5分钟检查一次
		ticker := time.NewTicker(interval)
		for range ticker.C {
			if isCompleted {
				log.Println("今日打款已完成，不再继续检查")
				ticker.Stop()
				break
			}
			log.Println("开始下一次定期检查")
			checkAndAlert(db)
		}
	}
}

func checkAndAlert(db *sql.DB) {
	// 如果已完成，则不再继续检查
	if isCompleted {
		log.Println("打款已完成，今日不再进行检查")
		return
	}

	log.Println("执行数据库查询...")
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

	log.Printf("查询结果：%d", count)

	// 如果count为0，表示打款未完成
	if count == 0 {
		if !alertSent {
			log.Println("打款未完成，发送告警")
			sendAlert("今日打款未完成，请尽快处理", true)
			alertSent = true // 记录已发送告警
		} else {
			log.Println("打款未完成，但告警已发送，等待30分钟后再次发送")
		}

		// 每30分钟发送一次告警
		time.AfterFunc(alertInterval, func() {
			log.Println("再次检查打款状态...")
			checkAndAlert(db) // 30分钟后再次检查
		})

	} else {
		// 打款已完成，发送消息
		log.Println("打款已完成，发送完成通知")
		sendAlert("今日打款已完成", false)
		isCompleted = true // 标记为已完成，停止后续检查
	}
}

func sendAlert(message string, needMentions bool) {
	log.Printf("发送告警消息：%s", message)
	sendToRobot(robot1URL, message, needMentions)
	sendToRobot(robot2URL, message, false) // 第二个机器人不需要@人
}

func sendToRobot(url, message string, needMentions bool) {
	msg := DingTalkMessage{
		MsgType: "text",
	}
	msg.Text.Content = message

	if needMentions && mentions != "" {
		msg.At.AtMobiles = parseMentions(mentions)
		msg.At.IsAtAll = false
		log.Printf("将会@以下手机号的用户: %v", msg.At.AtMobiles)
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
		log.Printf("钉钉响应非200状态：%s", resp.Status)
	} else {
		log.Println("告警消息发送成功")
	}
}

// 解析 -mentions 参数为手机号数组并添加 +86
func parseMentions(mentions string) []string {
	mobileNumbers := strings.Split(mentions, ",")
	for i, num := range mobileNumbers {
		num = strings.TrimSpace(num)
		if !strings.HasPrefix(num, "+") {
			// 如果手机号没有以+开头，默认添加 +86
			mobileNumbers[i] = "+86" + num
		}
	}
	return mobileNumbers
}

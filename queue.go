package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var dbNum = 13
var redis_client *redis.Client
var DefaultStandbySleep = 10 // default sleep time
var ConsumerGroupAlreadyExists = "BUSYGROUP Consumer Group name already exists"

type Message struct {
	Ctx       context.Context
	MessageID string
	Stream    string
	Value     map[string]interface{}
}

type Client struct {
	Key           string
	Stream        string                          // Redis stream key
	Group         string                          // Redis group key
	ConsumerKey   string                          // Redis consumer key
	ConsumerCount int                             // Consumer count
	ConsumerID    int                             // Consumer_id
	MessageCount  int64                           // Count of messages per read; default is one
	ClaimCount    int64                           // Count of messages per retry read; default is one
	Rate          time.Duration                   // Consume rate
	StandbySleep  int                             // Standby sleep time
	Block         time.Duration                   // Block sleep time
	MinIdle       time.Duration                   // Retry minimum idle time
	TraceCtx      context.Context                 // Trace context
	RedisClient   *redis.Client                   // Redis client (go-redis/v9)"
	HandleFunc    func(res []redis.XMessage) bool // Handle func
	RetryFunc     func(res []redis.XMessage) bool // Retry func
}

func GetRedisClient(host, port, password string, db int) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: password,
		DB:       db,
	})
	var ctx = context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal(fmt.Sprintf("GetRedisClient Fail：%v", err.Error()))
	}
	return rdb
}

func (client *Client) SetRedisClient(rdb *redis.Client) *Client {
	client.RedisClient = rdb
	return client
}

func (client *Client) SetHandleFunc(function func(res []redis.XMessage) bool) *Client {
	client.HandleFunc = function
	return client
}

func (client *Client) SetRetryFunc(function func(res []redis.XMessage) bool) *Client {
	client.RetryFunc = function
	return client
}

func (client *Client) SetStandbySleepTime(sleep int) *Client {
	client.StandbySleep = sleep
	return client
}

func (client *Client) SetRateTime(rate time.Duration) *Client {
	client.Rate = rate
	return client
}

func (client *Client) Init() *Client {
	client.Stream = client.Key + "_stream"
	client.Group = client.Key + "_group"
	client.ConsumerKey = client.Key + "_consumer"
	if client.StandbySleep == 0 {
		client.StandbySleep = DefaultStandbySleep
	}
	return client
}

func (client *Client) Start() {
	client.ConsumerGroupCreate()
	client.Consumer()
}

/*Create consumer group*/
func (client *Client) ConsumerGroupCreate() {
	var ctx = context.Background()
	err := client.RedisClient.XGroupCreateMkStream(ctx, client.Stream, client.Group, "0").Err()
	if err != nil && err.Error() != ConsumerGroupAlreadyExists {
		log.Fatal(err.Error())
	}
	for i := 0; i < client.ConsumerCount; i++ {
		err := client.RedisClient.XGroupCreateConsumer(ctx, client.Stream, client.Group, client.ConsumerKey+strconv.Itoa(i)).Err()
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

/*Producer*/
func (client *Client) Producer(values interface{}) (string, error) {
	res := client.RedisClient.XAdd(context.Background(), &redis.XAddArgs{
		Stream: client.Stream,
		Values: map[string]interface{}{
			"values": values,
		},
	})
	if res.Err() != nil {
		return "", res.Err()
	}
	return res.Val(), nil
}

/*Consumer*/
func (client *Client) Consumer() {
	var ctx, cancel = context.WithCancel(context.Background())
	client.TraceCtx = ctx
	defer func() {
		if err := recover(); err != nil {
			cancel()
			log.Fatal("stream:" + client.Stream + ";" + "group:" + client.Group + ";" + "error:" + err.(error).Error()) // 自定义异常处理
		}
	}()
	for i := 0; i < client.ConsumerCount; i++ {
		go client.TaskHandle(i)
	}
	go client.RetryHandle()
}

/*Task handle func*/
func (client *Client) TaskHandle(i int) {
	client.ConsumerID = i
	sleep := 1
	var ctx = context.Background()
	for true {
		select {
		case <-client.TraceCtx.Done():
			return
		default:
			res := client.RedisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    client.Group,
				Consumer: client.ConsumerKey + strconv.Itoa(client.ConsumerID),
				Streams:  []string{client.Stream, ">"},
				Count:    client.MessageCount,
				Block:    client.Block,
			}).Val()

			if len(res) == 0 || len(res[0].Messages) == 0 {
				time.Sleep(time.Duration(sleep) * time.Second)
				if sleep < client.StandbySleep {
					sleep++
				}
				continue
			} else {
				id := GetMessageID(res)
				if client.HandleFunc(res[0].Messages) {
					ack := client.RedisClient.XAck(ctx, client.Stream, client.Group, id).Val()
					if ack == 1 {
						client.RedisClient.XDel(ctx, client.Stream, id)
					}
				}
				sleep = 1
			}
		}
		if client.Rate > 0 {
			time.Sleep(client.Rate)
		}
	}
}

/*Task retry func*/
func (client *Client) RetryHandle() {
	sleep := 1
	var ctx = context.Background()

	for true {
		select {
		case <-client.TraceCtx.Done():

			return
		default:
			res, _ := client.RedisClient.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   client.Stream,
				Group:    client.Group,
				Consumer: client.ConsumerKey + "0",
				Start:    "0",
				Count:    client.ClaimCount,
				MinIdle:  client.MinIdle,
			}).Val()

			if len(res) == 0 {
				time.Sleep(time.Duration(sleep) * time.Second)
				if sleep < client.StandbySleep {
					sleep++
				}
				continue
			} else {
				if client.RetryFunc(res) {
					ack := client.RedisClient.XAck(ctx, client.Stream, client.Group, res[0].ID).Val()
					if ack == 1 {
						client.RedisClient.XDel(ctx, client.Stream, res[0].ID)
					}
					sleep = 1
				}
			}
		}
		if client.Rate > 0 {
			time.Sleep(client.Rate)
		}
	}
}

func GetMessageID(res []redis.XStream) string {
	if len(res[0].Messages) == 0 {
		return ""
	}
	return res[0].Messages[0].ID
}

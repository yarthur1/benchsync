package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "os"
    "path"
    "runtime"
    "time"

    "github.com/go-redis/redis/v8"
    "github.com/natefinch/lumberjack"
)

var ORDER string = "/data/benchsync/order.log"
var LOCAL string = "/data/benchsync/local.log"
var SYNC string = "/data/benchsync/sync.log"
var DETAIL string = "/data/benchsync/detail.log"
var detail_on bool = false
var colums int = 10000

const LOCAL_SYNC_KEY string = "yxj:local:flag"
const LOCAL_SYNC_DEL_FLAG string = "yxj:flag:delkey"

var logOrder *log.Logger
var logLocal *log.Logger
var logSync *log.Logger
var logDetail *log.Logger

var urlsLB []string = []string{"infra-codis-proxy-ibmams03-lb-1895889-ams03.clb.appdomain.cloud",
    "infra-codis-proxy-ibmsng01-lb-1895889-sng01.clb.appdomain.cloud",
    "infra-codis-proxy-ibmwdc04-lb-1895889-wdc04.clb.appdomain.cloud"}

var urls []string = []string{"10.136.138.130",
    "10.64.240.213",
    "10.148.78.81"}

var urls2 []string = []string{"10.136.138.152",
    "10.64.240.212",
    "10.148.78.107"}

var keys []string = []string{"ams:benchmark:test:qps:string:%d",
    "sng:benchmark:test:qps:string:%d",
    "wdc:benchmark:test:qps:string:%d"}

var url int
var client *redis.Client

var c *redis.Client   //标志位
var read bool         //读或写
var nums int = 1000   //消息数
var sleep int = 5     //等待 S
var wch int = 4       //写并发
var rch int = 16      //读并发
var testType int = 0  //0 延时 1 压力 2顺序
var orderType int = 0 //有序性测试  0 读 1 写
var proxy int = 1     //direct proxy num
var lb int = 1        //default use lb
var pool int = 40     //default connection pool size
var timeout int = 0   //-1  no timeout
var master int = 0   //0 ams 1 sng 2 wdc

func waitDelSync() { //key为2  continue
    for {
        r, _ := c.Get(context.Background(), keyGen(LOCAL_SYNC_DEL_FLAG)).Result()
        if r == "2" {
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func setDelFlag() {
    c.Incr(context.Background(), keyGen(LOCAL_SYNC_DEL_FLAG)).Result()
}

func unSetDelFlag() {
    c.Del(context.Background(), keyGen(LOCAL_SYNC_DEL_FLAG)).Result()
}

func Writeable() bool { //key不存在 0 可写
    res, _ := c.Exists(context.Background(), keyGen(LOCAL_SYNC_KEY)).Result()
    if res == 0 {
        return true
    }
    r, _ := c.Get(context.Background(), keyGen(LOCAL_SYNC_KEY)).Result()
    if r == "0" {
        return true
    }
    return false
}

func waitSync() { //key为0  continue
    for {
        r, _ := c.Get(context.Background(), keyGen(LOCAL_SYNC_KEY)).Result()
        if r == "0" {
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func readFlagSet() bool {
    res, _ := c.Decr(context.Background(), keyGen(LOCAL_SYNC_KEY)).Result()
    if res >= 0 { //返回减后的数字
        return true
    }
    return false
}

func disableWrite() bool {
    _, err := c.Set(context.Background(), keyGen(LOCAL_SYNC_KEY), "2", 0).Result()
    if err != nil {
        return false
    }
    return true
}

func Init() {
    flag.IntVar(&testType, "t", 0, "测试类型,默认延时  0 延时 1 压力 2 顺序")
    flag.IntVar(&orderType, "o", 0, "有序性测试,默认读  0 读 1 写")
    flag.BoolVar(&read, "r", true, "默认读Redis")
    flag.IntVar(&url, "u", 0, "url,默认 0 ams 1 sng 2 wdc")
    flag.IntVar(&nums, "n", 1000, "各类型消息数,默认 1000")
    flag.IntVar(&sleep, "s", 5, "休眠,默认 5S")
    flag.IntVar(&wch, "wc", 4, "写并发,默认 4")
    flag.IntVar(&rch, "rc", 16, "读并发,默认 16")
    flag.IntVar(&lb, "l", 1, "default use lb")
    flag.IntVar(&proxy, "p", 1, "direct proxy num, 默认 proxy 1")
    flag.IntVar(&pool, "a", 40, "alive max session(default connection pool size)")
    flag.IntVar(&timeout, "e", 0, "redis pool read timeout")
    flag.StringVar(&ORDER, "fo", "/data/benchsync/order.log", "order file name")
    flag.StringVar(&LOCAL, "fl", "/data/benchsync/local.log", "local file name")
    flag.StringVar(&SYNC, "fs", "/data/benchsync/sync.log", "sync file name")
    flag.StringVar(&DETAIL, "d", "/data/benchsync/detail.log", "detail file name")
    flag.BoolVar(&detail_on, "do", false, "detail log on or off")
    flag.IntVar(&colums, "c", 10000, "detail 每行的列数")
    flag.IntVar(&master, "m", 0, "哪个集群写，flag key不同")
    flag.Parse()

    var urlStr string = urlsLB[url]
    if lb == 0 {
        switch proxy {
        case 1:
            urlStr = urls[url]
        case 2:
            urlStr = urls2[url]
        }
    }

    fmt.Printf("type=%v,url=%v,read=%v,消息数=%v,休眠=%v,写并发=%v,读并发=%v,CPU:%v,pool:%v,timeout:%v\n", testType, urlStr, read, nums, sleep, wch, rch, runtime.NumCPU(), pool, timeout)
    fmt.Printf("order file=%v,local file=%v,sync file=%v\n", ORDER, LOCAL, SYNC)
    client = redis.NewClient(&redis.Options{
        Addr:         urlStr + ":19000",
        Password:     "", // no password set
        DB:           0,  // use default DB
        PoolSize:     pool,
        MinIdleConns: pool,
        ReadTimeout:  time.Duration(timeout),
    })
    c = redis.NewClient(&redis.Options{
        Addr:     "10.64.240.246:6479",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    os.MkdirAll(path.Dir(ORDER), 0755)
    os.MkdirAll(path.Dir(LOCAL), 0755)
    os.MkdirAll(path.Dir(SYNC), 0755)
    hook := &lumberjack.Logger{
        Filename:   ORDER, //filePath
        MaxSize:    50,    // megabytes
        MaxBackups: 1,
        MaxAge:     30,    //days
        Compress:   false, // disabled by default
    }
    defer hook.Close()
    logOrder = log.New(hook, "", log.LstdFlags)

    hookLocal := &lumberjack.Logger{
        Filename:   LOCAL, //filePath
        MaxSize:    50,    // megabytes
        MaxBackups: 1,
        MaxAge:     30,    //days
        Compress:   false, // disabled by default
    }
    defer hookLocal.Close()
    logLocal = log.New(hookLocal, "", log.LstdFlags)

    hookSync := &lumberjack.Logger{
        Filename:   SYNC, //filePath
        MaxSize:    50,   // megabytes
        MaxBackups: 1,
        MaxAge:     30,    //days
        Compress:   false, // disabled by default
    }
    defer hookSync.Close()
    logSync = log.New(hookSync, "", log.LstdFlags)

    hookDetail := &lumberjack.Logger{
        Filename:   DETAIL, //filePath
        MaxSize:    50,   // megabytes
        MaxBackups: 1,
        MaxAge:     30,    //days
        Compress:   false, // disabled by default
    }
    logDetail = log.New(hookDetail, "", log.LstdFlags)
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())
    var i int64 = 0
    Init()
    for {
        i++
        switch testType {
        case 0:
            if !read {
                for {
                    if Writeable() {
                        break
                    }
                    time.Sleep(1 * time.Second)
                }

                local()
                time.Sleep(time.Duration(sleep) * time.Second)
                disableWrite()
                fmt.Printf("第%d次发送数据，sleep 10S to wait whether stop\n", i)
                time.Sleep(10 * time.Second)
            } else {
                for {
                    if !Writeable() {
                        break
                    }
                    time.Sleep(1 * time.Second)
                }
                readSync()
                setDelFlag()
                waitDelSync()
                time.Sleep(5 * time.Second) //一定要保证删除之前wait break
                unSetDelFlag()

                delSyncKey(nums)
                time.Sleep(time.Duration(sleep) * time.Second)
                readFlagSet()
                waitSync()
                fmt.Printf("第%v次read done\n", i)
            }
        case 1:
            benchSetFunc(nums)
        case 2:
            if orderType == 0 { //顺序测试  读
                for {
                    if orderReadable() {
                        break
                    }
                    time.Sleep(1 * time.Second)
                }
                OrderBenchRead()
                syncSet(keyGen(ORDER_WAIT_FLAG))
                syncWait(keyGen(ORDER_WAIT_FLAG))
                fmt.Printf("第%v次wait break\n", i)
                time.Sleep(5 * time.Second)
                syncUnSet(keyGen(ORDER_WAIT_FLAG)) //一定要保证删除之前wait break

                OrderBenchDel()
                orderReadFlagSet()
                waitReadSync()
                fmt.Printf("第%v次read done\n", i)
            } else {
                for {
                    if orderWriteable() {
                        break
                    }
                    time.Sleep(1 * time.Second)
                }
                OrderBenchWrite()
                time.Sleep(time.Duration(sleep) * time.Second)
                disableOrderWrite()
                fmt.Printf("第%d次发送数据，sleep 10S to wait whether stop\n", i)
                time.Sleep(10 * time.Second)
            }
        }
    }
}

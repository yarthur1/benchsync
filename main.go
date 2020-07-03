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

const ORDER string = "/data/benchsync/order.log"
const LOCAL string = "/data/benchsync/local.log"
const SYNC string = "/data/benchsync/sync.log"

var logOrder *log.Logger
var logLocal *log.Logger
var logSync *log.Logger

var urlsLB []string = []string{"infra-codis-proxy-ibmams03-lb-1895889-ams03.clb.appdomain.cloud",
   "infra-codis-proxy-ibmsng01-lb-1895889-sng01.clb.appdomain.cloud",
   "infra-codis-proxy-ibmwdc04-lb-1895889-wdc04.clb.appdomain.cloud"}

var urls []string = []string{"10.136.138.130",
    "10.64.240.213",
    "10.148.78.81"}

var keys []string = []string{"ams:benchmark:test:qps:string:%d",
    "sng:benchmark:test:qps:string:%d",
    "wdc:benchmark:test:qps:string:%d"}

var url int
var client *redis.Client

var c *redis.Client  //标志位
var read bool        //读或写
var nums int = 1000  //消息数
var sleep int = 5    //等待 S
var wch int = 4      //写并发
var rch int = 16     //读并发
var testType int = 0 //0 延时 1 压力&顺序
var orderType int = 0 //有序性测试  0 读 1 写

func Writeable() bool { //key不存在 可写
    res, _ := c.Exists(context.Background(), "flag:key").Result()
    if res == 0 {
        return true
    }
    return false
}

func enableWrite() bool {
    res, _ := c.Del(context.Background(), "flag:key").Result()
    if res == 1 {
        return true
    }
    return false
}

func disableWrite() bool {
    _, err := c.Set(context.Background(), "flag:key", "true", 0).Result()
    if err != nil {
        return false
    }
    return true
}

func Init() {
    flag.IntVar(&testType, "t", 0, "测试类型,默认延时  0 延时 1 压力&顺序")
    flag.IntVar(&orderType, "o", 0, "有序性测试,默认读  0 读 1 写")
    flag.BoolVar(&read, "r", true, "默认读Redis")
    flag.IntVar(&url, "u", 0, "url,默认 0 ams 1 sng 2 wdc")
    flag.IntVar(&nums, "n", 1000, "各类型消息数,默认 1000")
    flag.IntVar(&sleep, "s", 5, "休眠,默认 5S")
    flag.IntVar(&wch, "wc", 4, "写并发,默认 4")
    flag.IntVar(&rch, "rc", 16, "读并发,默认 16")
    flag.Parse()

    var urlStr string = urls[url]
    if testType != 0{
        urlStr = urlsLB[url]
    }
    fmt.Printf("type=%v,url=%v,read=%v,消息数=%v,休眠=%v,写并发=%v,读并发=%v\n", testType, urls[url], read, nums, sleep, wch, rch)
    client = redis.NewClient(&redis.Options{
        Addr:     urlStr + ":19000",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
    c = redis.NewClient(&redis.Options{
        Addr:     "10.64.240.246:6479",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    os.MkdirAll(path.Dir(ORDER), 0755)
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
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())
    var i int64 = 0
    Init()
    for {
        switch testType{
            case 0:
                if !read {
                    for {
                        if Writeable() {
                            break
                        }
                        time.Sleep(3 * time.Second)
                    }
                    local()
                    time.Sleep(time.Duration(sleep) * time.Second)
                    disableWrite()
                } else {
                    i++
                    for {
                        if !Writeable() {
                            break
                        }
                        time.Sleep(3 * time.Second)
                    }
                    readSync()
                    delSyncKey(nums)
                    time.Sleep(time.Duration(sleep) * time.Second)
                    enableWrite()
                    fmt.Printf("第%d次接收数据，sleep等待del同步\n", i)
                    time.Sleep(time.Duration(5) * time.Second)
                }
            case 1:
                benchSetFunc(nums)
                //if orderType==0{
                //    for {
                //        if !orderWriteable() {
                //            break
                //        }
                //        time.Sleep(3 * time.Second)
                //    }
                //
                //}else{
                //
                //}
        }
    }
}

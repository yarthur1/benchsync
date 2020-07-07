package main

import (
    "context"
    "fmt"
    "strconv"
    "sync"
    "time"
)

const FLAG_ORDER string = "yxj:flag:order"
const KEY_ORDER string = "yxj:order:key"

func benchSetFunc(n int) {
    var w sync.WaitGroup
    ch := make(chan struct{}, wch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go benchSetRoutine(i, ch, &w)
    }
    w.Wait()
}

func benchSetRoutine(i int, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := fmt.Sprintf(keys[url], i)
    client.Set(context.Background(), key, "qps test", 0).Result()
}

func orderWriteable() bool { //key不存在或为0  可写
    res, _ := c.Exists(context.Background(), FLAG_ORDER).Result()
    if res == 0 {
        return true
    }
    r, _ := c.Get(context.Background(), FLAG_ORDER).Result()
    if r == "0" {
        return true
    }
    return false
}

func orderReadable() bool { //key为1,2  可读
    res, _ := c.Exists(context.Background(), FLAG_ORDER).Result()
    if res == 0 {
        return false
    }
    r, _ := c.Get(context.Background(), FLAG_ORDER).Result()
    if r == "1" || r == "2" {
        return true
    }
    return false
}

func waitReadSync() { //key为0  continue
    for {
        r, _ := c.Get(context.Background(), FLAG_ORDER).Result()
        if r == "0" {
            break
        }
        time.Sleep(1 * time.Second)
    }
}

func orderReadFlagSet() bool {
    res, _ := c.Decr(context.Background(), FLAG_ORDER).Result()
    if res >= 0 { //返回减后的数字
        return true
    }
    return false
}

func disableOrderWrite() bool {
    _, err := c.Set(context.Background(), FLAG_ORDER, "2", 0).Result()
    if err != nil {
        return false
    }
    return true
}

func OrderBenchWrite() {
    conn := client.Conn(context.Background())
    for i := 0; i < nums; i++ {
        tmp := strconv.Itoa(i)
        _, err := conn.Append(context.Background(), KEY_ORDER, tmp).Result()
        if err != nil {
            logOrder.Printf("OrderBenchWrite failed, err:%v\n", err)
            break
        }
    }
}

func OrderBenchRead() {
    res, err := client.Get(context.Background(), KEY_ORDER).Result()
    if err != nil {
        logOrder.Printf("Read redis failed, err:%v\n", err)
        return
    }
    var str string = ""
    for i := 0; i < nums; i++ {
        tmp := strconv.Itoa(i)
        str = str + tmp
    }
    if res == str {
        logOrder.Printf("OrderBench test success\n")
    } else {
        logOrder.Printf("OrderBench test failed,result:%s\n", res)
    }
}

func OrderBenchDel() {
    _, err := client.Del(context.Background(), KEY_ORDER).Result()
    if err != nil {
        logOrder.Printf("OrderBenchDel failed, err:%v\n", err)
    }
}
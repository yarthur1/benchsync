package main

import (
    "context"
    "fmt"
    "strconv"
    "sync"
)

const FLAG_ORDER string= "flag_order"
const KEY_ORDER string= "order_key"

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

func benchSetRoutine(i int, ch chan struct{}, w *sync.WaitGroup){
    defer func() {
        <- ch
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
    if r=="0"{
        return true
    }
    return false
}

func orderRead() bool {
    res, _ := c.Decr(context.Background(), FLAG_ORDER).Result()
    if res >= 0 {       //返回减后的数字
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

func OrderBenchWrite(){
    conn:=client.Conn(context.Background())
    var str string = ""
    for i:=0;i<10000;i++{
        tmp :=strconv.Itoa(i)
        conn.Append(context.Background(), KEY_ORDER, tmp)
        str =str+tmp
    }
}
package main

import (
    "context"
    "fmt"
    "sort"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
)

var syncTimeString []int
var syncTimeHash []int
var syncTimeSet []int
var syncTimeList []int

var errStringSync int32
var errHashSync int
var errSetSync int
var errListSync int

func getFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    syncTimeString = make([]int, 0, n)
    errStringSync = 0

    var w sync.WaitGroup
    var mu sync.Mutex
    ch := make(chan struct{}, rch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go getRoutine(i, &mu, ch, &w)
    }
    w.Wait()
}

func getRoutine(i int, m *sync.Mutex, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
    res, err := client.Get(context.Background(), key).Result()
    if err != nil {
        atomic.AddInt32(&errStringSync, 1)
        return
    }
    split := strings.Split(res, "\t")
    start, _ := strconv.ParseInt(split[0], 10, 64)
    end, _ := strconv.ParseInt(split[1], 10, 64)
    t := int((end - start) / 1e6)

    m.Lock()
    syncTimeString = append(syncTimeString, t)
    m.Unlock()
}

func hgetallFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    syncTimeHash = make([]int, 0, n)
    errHashSync = 0
    key := BENCHMARK_HASH_KEY
    res, _ := client.HGetAll(context.Background(), key).Result()
    for _, v := range res {
        split := strings.Split(v, "\t")
        start, _ := strconv.ParseInt(split[0], 10, 64)
        end, _ := strconv.ParseInt(split[1], 10, 64)
        syncTimeHash = append(syncTimeHash, int((end-start)/1e6))
    }
    errHashSync = n - len(syncTimeHash)
}

func lrangeFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    syncTimeList = make([]int, 0, n)
    errListSync = 0
    key := BENCHMARK_LIST_KEY
    res, _ := client.LRange(context.Background(), key, 0, -1).Result()
    for _, v := range res {
        split := strings.Split(v, "\t")
        start, _ := strconv.ParseInt(split[0], 10, 64)
        end, _ := strconv.ParseInt(split[1], 10, 64)
        syncTimeList = append(syncTimeList, int((end-start)/1e6))
    }
    errListSync = n - len(syncTimeList)
}

func smemberFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    syncTimeSet = make([]int, 0, n)
    errSetSync = 0
    key := BENCHMARK_SET_KEY
    res, _ := client.SMembers(context.Background(), key).Result()
    for _, v := range res {
        split := strings.Split(v, "\t")
        start, _ := strconv.ParseInt(split[0], 10, 64)
        end, _ := strconv.ParseInt(split[1], 10, 64)
        syncTimeSet = append(syncTimeSet, int((end-start)/1e6))
    }
    errSetSync = n - len(syncTimeSet)
}

func readSync() {
    var wg sync.WaitGroup
    wg.Add(4)
    go getFunc(nums, &wg)
    go hgetallFunc(nums, &wg)
    go lrangeFunc(nums, &wg)
    go smemberFunc(nums, &wg)
    wg.Wait()

    sort.Ints(syncTimeString)
    l := len(syncTimeString)
    p50 := (l - 1) / 2
    p90 := ((l - 1) * 90) / 100
    p99 := ((l - 1) * 99) / 100
    p39 := ((l - 1) * 999) / 1000
    p49 := ((l - 1) * 9999) / 10000
    logSync.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, syncTimeString[p50], syncTimeString[p90], syncTimeString[p99], syncTimeString[p39], syncTimeString[p49], syncTimeString[l-1], errStringSync)

    sort.Ints(syncTimeHash)
    l = len(syncTimeHash)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logSync.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, syncTimeHash[p50], syncTimeHash[p90], syncTimeHash[p99], syncTimeHash[p39], syncTimeHash[p49], syncTimeHash[l-1], errHashSync)

    sort.Ints(syncTimeSet)
    l = len(syncTimeSet)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logSync.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, syncTimeSet[p50], syncTimeSet[p90], syncTimeSet[p99], syncTimeSet[p39], syncTimeSet[p49], syncTimeSet[l-1], errSetSync)

    sort.Ints(syncTimeList)
    l = len(syncTimeList)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logSync.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, syncTimeList[p50], syncTimeList[p90], syncTimeList[p99], syncTimeList[p39], syncTimeList[p49], syncTimeList[l-1], errListSync)
}

func delSyncKey(n int) {
    var w sync.WaitGroup
    ch := make(chan struct{}, rch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go delRoutine(i, ch, &w)
    }

    client.Del(context.Background(), BENCHMARK_HASH_KEY).Result()
    client.Del(context.Background(), BENCHMARK_SET_KEY).Result()
    client.Del(context.Background(), BENCHMARK_LIST_KEY).Result()
    w.Wait()
}

func delRoutine(i int, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
    client.Del(context.Background(), key).Result()
}

package main

import (
    "context"
    "fmt"
    "sort"
    "strconv"
    "strings"
    "sync"
)

var syncTimeString []int
var syncTimeHash []int
var syncTimeSet []int
var syncTimeList []int

var errStringSync int
var errHashSync int
var errSetSync int
var errListSync int

func getFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    syncTimeString = make([]int, 0, n)
    errStringSync = 0
    for i := 0; i < n; i++ {
        key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
        res, err := client.Get(context.Background(), key).Result()
        if err != nil {
            errStringSync++
            continue
        }
        split := strings.Split(res, "\t")
        start, _ := strconv.ParseInt(split[0], 10, 64)
        end, _ := strconv.ParseInt(split[1], 10, 64)
        syncTimeString = append(syncTimeString, int((end-start)/1e6))
    }
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
    logSync.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, syncTimeString[p50], syncTimeString[p90], syncTimeString[p99], syncTimeString[l-1], errStringSync)

    sort.Ints(syncTimeHash)
    l = len(syncTimeHash)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logSync.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, syncTimeHash[p50], syncTimeHash[p90], syncTimeHash[p99], syncTimeHash[l-1], errHashSync)

    sort.Ints(syncTimeSet)
    l = len(syncTimeSet)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logSync.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, syncTimeSet[p50], syncTimeSet[p90], syncTimeSet[p99], syncTimeSet[l-1], errSetSync)

    sort.Ints(syncTimeList)
    l = len(syncTimeList)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logSync.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, syncTimeList[p50], syncTimeList[p90], syncTimeList[p99], syncTimeList[l-1], errListSync)
}

func delSyncKey(n int) {
    for i := 0; i < n; i++ {
        key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
        client.Del(context.Background(), key).Result()
    }
    client.Del(context.Background(), BENCHMARK_HASH_KEY).Result()
    client.Del(context.Background(), BENCHMARK_SET_KEY).Result()
    client.Del(context.Background(), BENCHMARK_LIST_KEY).Result()
}
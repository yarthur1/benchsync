package main

import (
    "context"
    "fmt"
    "sort"
    "strconv"
    "sync"
    "time"
)

const (
    BENCHMARK_STRING_KEY = "benchmark:test:string:%d"
    BENCHMARK_HASH_KEY   = "benchmark:test:hash"
    BENCHMARK_SET_KEY    = "benchmark:test:set"
    BENCHMARK_LIST_KEY   = "benchmark:test:list"
)

var localTimeString []int
var localTimeHash []int
var localTimeSet []int
var localTimeList []int

var errString int
var errHash int
var errSet int
var errList int

func setFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeString = make([]int, 0, n)
    errString = 0
    for i := 0; i < n; i++ {
        key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
        start := time.Now()
        _, err := client.Set(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10), 0).Result()
        t := int(time.Since(start) / 1e6)
        localTimeString = append(localTimeString, t)
        if err != nil {
            errString++
        }
    }
}

func hmsetFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeHash = make([]int, 0, n)
    errHash = 0
    for i := 0; i < n; i++ {
        key := BENCHMARK_HASH_KEY
        start := time.Now()
        _, err := client.HMSet(context.Background(), key, i, strconv.FormatInt(start.UnixNano(), 10)).Result()
        t := int(time.Since(start) / 1e6)
        localTimeHash = append(localTimeHash, t)
        if err != nil {
            errHash++
        }
    }
}

func lpushFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeList = make([]int, 0, n)
    errList = 0
    for i := 0; i < n; i++ {
        key := BENCHMARK_LIST_KEY
        start := time.Now()
        _, err := client.LPush(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10)).Result()
        t := int(time.Since(start) / 1e6)
        localTimeList = append(localTimeList, t)
        if err != nil {
            errList++
        }
    }
}

func saddFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeSet = make([]int, 0, n)
    errSet = 0
    for i := 0; i < n; i++ {
        key := BENCHMARK_SET_KEY
        start := time.Now()
        _, err := client.SAdd(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10)).Result()
        t := int(time.Since(start) / 1e6)
        localTimeSet = append(localTimeSet, t)
        if err != nil {
            errSet++
        }
    }
}

func local() {
    var wg sync.WaitGroup
    wg.Add(4)
    go setFunc(nums, &wg)
    go hmsetFunc(nums, &wg)
    go lpushFunc(nums, &wg)
    go saddFunc(nums, &wg)
    wg.Wait()

    sort.Ints(localTimeString)
    l := len(localTimeString)
    p50 := (l - 1) / 2
    p90 := ((l - 1) * 90) / 100
    p99 := ((l - 1) * 99) / 100
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, localTimeString[p50], localTimeString[p90], localTimeString[p99], localTimeString[l-1], errString)

    sort.Ints(localTimeHash)
    l = len(localTimeHash)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, localTimeHash[p50], localTimeHash[p90], localTimeHash[p99], localTimeHash[l-1], errHash)

    sort.Ints(localTimeSet)
    l = len(localTimeSet)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, localTimeSet[p50], localTimeSet[p90], localTimeSet[p99], localTimeSet[l-1], errSet)

    sort.Ints(localTimeList)
    l = len(localTimeList)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d max:%d err:%d\n", l, localTimeList[p50], localTimeList[p90], localTimeList[p99], localTimeList[l-1], errList)
}

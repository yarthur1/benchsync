package main

import (
    "context"
    "fmt"
    "sort"
    "strconv"
    "sync"
    "sync/atomic"
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

var errString int32
var errHash int32
var errSet int32
var errList int32

func setFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeString = make([]int, 0, n)
    errString = 0

    var w sync.WaitGroup
    var mu sync.Mutex
    ch := make(chan struct{}, wch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go setRoutine(i, &mu, ch, &w)
    }
    w.Wait()
}

func setRoutine(i int, m *sync.Mutex, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := fmt.Sprintf(BENCHMARK_STRING_KEY, i)
    key = keyGen(key)
    start := time.Now()
    _, err := client.Set(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10), 0).Result()
    t := int(time.Since(start) / 1e6)

    m.Lock()
    localTimeString = append(localTimeString, t)
    m.Unlock()
    if err != nil {
        atomic.AddInt32(&errString, 1)
    }
}

func hmsetFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeHash = make([]int, 0, n)
    errHash = 0

    var w sync.WaitGroup
    var mu sync.Mutex
    ch := make(chan struct{}, wch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go hsetRoutine(i, &mu, ch, &w)
    }
    w.Wait()
}

func hsetRoutine(i int, m *sync.Mutex, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := BENCHMARK_HASH_KEY
    key = keyGen(key)
    start := time.Now()
    _, err := client.HMSet(context.Background(), key, i, strconv.FormatInt(start.UnixNano(), 10)).Result()
    t := int(time.Since(start) / 1e6)

    m.Lock()
    localTimeHash = append(localTimeHash, t)
    m.Unlock()
    if err != nil {
        atomic.AddInt32(&errHash, 1)
    }
}

func lpushFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeList = make([]int, 0, n)
    errList = 0

    var w sync.WaitGroup
    var mu sync.Mutex
    ch := make(chan struct{}, wch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go lpushRoutine(i, &mu, ch, &w)
    }
    w.Wait()
}

func lpushRoutine(i int, m *sync.Mutex, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := BENCHMARK_LIST_KEY
    key = keyGen(key)
    start := time.Now()
    _, err := client.LPush(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10)).Result()
    t := int(time.Since(start) / 1e6)

    m.Lock()
    localTimeList = append(localTimeList, t)
    m.Unlock()
    if err != nil {
        atomic.AddInt32(&errList, 1)
    }
}

func saddFunc(n int, wg *sync.WaitGroup) {
    defer func() {
        wg.Done()
    }()

    localTimeSet = make([]int, 0, n)
    errSet = 0

    var w sync.WaitGroup
    var mu sync.Mutex
    ch := make(chan struct{}, wch)
    w.Add(n)

    for i := 0; i < n; i++ {
        ch <- struct{}{}
        go saddRoutine(i, &mu, ch, &w)
    }
    w.Wait()
}

func saddRoutine(i int, m *sync.Mutex, ch chan struct{}, w *sync.WaitGroup) {
    defer func() {
        <-ch
        w.Done()
    }()

    key := BENCHMARK_SET_KEY
    key = keyGen(key)
    start := time.Now()
    _, err := client.SAdd(context.Background(), key, strconv.FormatInt(start.UnixNano(), 10)).Result()
    t := int(time.Since(start) / 1e6)

    m.Lock()
    localTimeSet = append(localTimeSet, t)
    m.Unlock()
    if err != nil {
        atomic.AddInt32(&errSet, 1)
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

    if len(localTimeString) == 0 || len(localTimeHash) == 0 ||
            len(localTimeSet) == 0 || len(localTimeList) == 0 {
        return
    }

    sort.Ints(localTimeString)
    l := len(localTimeString)
    p50 := (l - 1) / 2
    p90 := ((l - 1) * 90) / 100
    p99 := ((l - 1) * 99) / 100
    p39 := ((l - 1) * 999) / 1000
    p49 := ((l - 1) * 9999) / 10000
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, localTimeString[p50], localTimeString[p90], localTimeString[p99], localTimeString[p39], localTimeString[p49], localTimeString[l-1], errString)

    sort.Ints(localTimeHash)
    l = len(localTimeHash)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, localTimeHash[p50], localTimeHash[p90], localTimeHash[p99], localTimeHash[p39], localTimeHash[p49], localTimeHash[l-1], errHash)

    sort.Ints(localTimeSet)
    l = len(localTimeSet)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, localTimeSet[p50], localTimeSet[p90], localTimeSet[p99], localTimeSet[p39], localTimeSet[p49], localTimeSet[l-1], errSet)

    sort.Ints(localTimeList)
    l = len(localTimeList)
    p50 = (l - 1) / 2
    p90 = ((l - 1) * 90) / 100
    p99 = ((l - 1) * 99) / 100
    p39 = ((l - 1) * 999) / 1000
    p49 = ((l - 1) * 9999) / 10000
    logLocal.Printf("n:%d 50:%d 90:%d 99:%d 99.9:%d 99.99:%d max:%d err:%d\n", l, localTimeList[p50], localTimeList[p90], localTimeList[p99], localTimeList[p39], localTimeList[p49], localTimeList[l-1], errList)
}

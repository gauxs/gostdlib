package main_test

import (
	"database/sql"
	"fmt"
	"gostdlib/trees/segment"
	"testing"
	"unsafe"

	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	_ "github.com/go-sql-driver/mysql"
)

// RANGE
type CustomIntRange int

func (ci CustomIntRange) Min(other segment.Range) segment.Range {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return nil
	}

	if ci <= otherCustomIntRange {
		return ci
	} else {
		return otherCustomIntRange
	}
}

func (ci CustomIntRange) Max(other segment.Range) segment.Range {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return nil
	}

	if ci >= otherCustomIntRange {
		return ci
	} else {
		return otherCustomIntRange
	}
}

func (ci CustomIntRange) Value() interface{} {
	return int(ci)
}

func (lRange CustomIntRange) SplitInEqualRange(other segment.Range) (segment.Range, segment.Range) {
	rRange, ok := other.(CustomIntRange)
	if !ok {
		return nil, nil
	}

	mid := (lRange + rRange) / 2
	return mid, mid + 1
}

func (ci CustomIntRange) IsEqualTo(other segment.Range) bool {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return false
	}

	return ci == otherCustomIntRange
}

func (ci CustomIntRange) IsLesserThan(other segment.Range) bool {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return false
	}

	return ci < otherCustomIntRange
}

func (ci CustomIntRange) IsGreaterThan(other segment.Range) bool {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return false
	}

	return ci > otherCustomIntRange
}

func (ci CustomIntRange) IsLesserThanEqualTo(other segment.Range) bool {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return false
	}

	return ci <= otherCustomIntRange
}

func (ci CustomIntRange) IsGreaterThanEqualTo(other segment.Range) bool {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return false
	}

	return ci >= otherCustomIntRange
}

func (ci CustomIntRange) Subtract(other segment.Range) segment.Range {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return nil
	}

	return ci - otherCustomIntRange
}

func (ci CustomIntRange) Add(other segment.Range) segment.Range {
	otherCustomIntRange, ok := other.(CustomIntRange)
	if !ok {
		return nil
	}

	return ci + otherCustomIntRange
}

func (ci CustomIntRange) Increment() segment.Range {
	return ci + 1
}

// RAW DATA
type MyRawData struct {
	data int
}

func (raw *MyRawData) Transform() (segment.SegmentData, error) {
	return &CustomIntData{
		data: raw.data,
	}, nil
}

// DATA
type CustomIntData struct {
	isCached bool
	data     int
	key      string
}

func (cid *CustomIntData) IsCached() bool {
	return cid.isCached
}

func (cid *CustomIntData) CacheMe() error {
	cid.isCached = true
	newUUID, _ := uuid.NewUUID()
	cid.key = newUUID.String()
	err := redisCache.Set(ctx, cid.key, cid.data, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (cid *CustomIntData) Get() (interface{}, error) {
	var value int
	if cid.IsCached() {
		val, err := redisCache.Get(ctx, cid.key).Result()
		if err != nil {
			panic(err)
		}

		value, _ = strconv.Atoi(val)
	} else {
		value = cid.data
	}

	return value, nil
}

func (cid *CustomIntData) Merge(other segment.SegmentData) segment.SegmentData {
	var value1, value2 int
	assertedOther := other.(*CustomIntData)
	if cid.IsCached() {
		val, err := redisCache.Get(ctx, cid.key).Result()
		if err != nil {
			panic(err)
		}
		value1, _ = strconv.Atoi(val)
	} else {
		value1 = cid.data
	}

	if assertedOther.IsCached() {
		val, err := redisCache.Get(ctx, assertedOther.key).Result()
		if err != nil {
			panic(err)
		}
		value2, _ = strconv.Atoi(val)
	} else {
		value2 = assertedOther.data
	}

	mergedValue := value1 + value2

	return &CustomIntData{
		data: mergedValue,
	}
}

// RESOLVER
type CustomIntResolver struct {
	database       *sql.DB
	statement      *sql.Stmt
	testDataSource []int
}

func (cir *CustomIntResolver) Init() {
	var err error
	cir.database, err = sql.Open("mysql", "gaux:dontenter@/segment")
	if err != nil {
		panic(err.Error())
	}

	err = cir.database.Ping()
	if err != nil {
		panic(err.Error())
	}

	cir.statement, err = cir.database.Prepare("SELECT Value FROM Basic WHERE ID = ?")
	if err != nil {
		panic(err.Error())
	}

	return
}

func (cir *CustomIntResolver) Close() {
	cir.database.Close()
	cir.statement.Close()
}

func (cir *CustomIntResolver) Resolve(lRange segment.Range, rRange segment.Range) segment.RawData {
	var err error
	var value int
	lInt := lRange.Value().(int)

	err = cir.statement.QueryRow(lInt).Scan(&value)
	if err != nil {
		panic(err.Error())
	}

	return &MyRawData{
		data: value,
	}
}

var ctx context.Context
var redisCache *redis.Client
var segmentTree *segment.SegmentTree

func BenchmarkBasicGet(t *testing.B) {
	ctx = context.Background()
	redisCache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	resolver := &CustomIntResolver{}
	resolver.Init()

	var lRange, rRange CustomIntRange
	lRange = CustomIntRange(1)
	rRange = CustomIntRange(10000)
	segmentTree = segment.NewSegmentTree(10000)
	rootNode := segment.NewSegmentNode(lRange, rRange)
	segmentTree.SetRoot(rootNode)
	segmentTree.SetResolver(resolver)

	var queryLRange, queryRRange CustomIntRange
	queryLRange = CustomIntRange(1)
	queryRRange = CustomIntRange(10000)
	cacheHit, cacheMiss, resultSegmentData := segmentTree.Get(queryLRange, queryRRange)
	data, _ := resultSegmentData.Get()

	fmt.Printf("size of single node: %d\n", unsafe.Sizeof(*rootNode))
	fmt.Printf("total number of node(s): %d\n", segmentTree.CountNodes())
	fmt.Printf("redisCache Hits: %d\n", cacheHit)
	fmt.Printf("redisCache Miss: %d\n", cacheMiss)
	fmt.Printf("Result: %d\n", data)
}

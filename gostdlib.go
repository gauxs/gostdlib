package main

import (
	"context"
	"database/sql"
	"gostdlib/trees/segment"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	_ "github.com/go-sql-driver/mysql"
)

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
	err := Cache.Set(CTX, cid.key, cid.data, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (cid *CustomIntData) Get() (interface{}, error) {
	var value int
	if cid.IsCached() {
		val, err := Cache.Get(CTX, cid.key).Result()
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
		val, err := Cache.Get(CTX, cid.key).Result()
		if err != nil {
			panic(err)
		}
		value1, _ = strconv.Atoi(val)
	} else {
		value1 = cid.data
	}

	if assertedOther.IsCached() {
		val, err := Cache.Get(CTX, assertedOther.key).Result()
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

type MyRawData struct {
	data int
}

func (raw *MyRawData) Transform() (segment.SegmentData, error) {
	return &CustomIntData{
		data: raw.data,
	}, nil
}

var Cache *redis.Client
var CTX context.Context
var segmentTree *segment.SegmentTree

func main() {
	CTX = context.Background()
	Cache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	var dataSource []int
	for i := 1; i <= 20; i++ {
		dataSource = append(dataSource, i)
	}

	resolver := &CustomIntResolver{
		testDataSource: dataSource,
	}

	resolver.Init()

	var lRange, rRange CustomIntRange
	lRange = CustomIntRange(1)
	rRange = CustomIntRange(20)
	segmentTree = segment.NewSegmentTree(20)
	rootNode := segment.NewSegmentNode(lRange, rRange)

	segmentTree.SetRoot(rootNode)
	segmentTree.SetResolver(resolver)

	// var queryLRange, queryRRange CustomIntRange
	// queryLRange = CustomIntRange(5)
	// queryRRange = CustomIntRange(15)
	// cacheHit, cacheMiss, resultSegmentData := segmentTree.Get(queryLRange, queryRRange)
	// data, _ := resultSegmentData.Get()
	// fmt.Printf("Cache Hits: %d\n", cacheHit)
	// fmt.Printf("Cache Miss: %d\n", cacheMiss)
	// fmt.Printf("Result: %d\n", data)

	// queryLRange = CustomIntRange(1)
	// queryRRange = CustomIntRange(10)
	// cacheHit, cacheMiss, resultSegmentData = segmentTree.Get(queryLRange, queryRRange)
	// data, _ = resultSegmentData.Get()
	// fmt.Printf("Cache Hits: %d\n", cacheHit)
	// fmt.Printf("Cache Miss: %d\n", cacheMiss)
	// fmt.Printf("Result: %d\n", data)

	ginEngine := gin.Default()
	ginEngine.GET("/segment", Segment)
	ginRunErr := ginEngine.Run("localhost" + ":" + "7890")

	if ginRunErr != nil {
		panic(ginRunErr)
	}

	resolver.Close()
}

func Segment(ginC *gin.Context) {
	urlParam := ginC.Request.URL.Query()
	l, _ := strconv.Atoi(urlParam["lRange"][0])
	r, _ := strconv.Atoi(urlParam["rRange"][0])

	var queryLRange, queryRRange CustomIntRange
	result := make(map[string]interface{})
	queryLRange = CustomIntRange(l)
	queryRRange = CustomIntRange(r)
	cacheHit, cacheMiss, resultSegmentData := segmentTree.Get(queryLRange, queryRRange)
	data, _ := resultSegmentData.Get()
	// fmt.Printf("Cache Hits: %d\n", cacheHit)
	// fmt.Printf("Cache Miss: %d\n", cacheMiss)
	// fmt.Printf("Result: %d\n", data)

	result["Cache Hits"] = cacheHit
	result["Cache Miss"] = cacheMiss
	result["Result"] = data

	ginC.JSON(http.StatusOK, result)
}

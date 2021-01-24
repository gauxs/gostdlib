package segment

import (
	"errors"
	"fmt"
	"strconv"
)

func NewSegmentNode(lRange Range, rRange Range) *SegmentNode {
	return &SegmentNode{
		lRange: lRange,
		rRange: rRange,
		data:   nil,
		left:   nil,
		right:  nil,
	}
}

type SegmentNode struct {
	// left range covered by this node
	lRange Range
	// right range covered by this node
	rRange Range
	// data in thisnode
	data SegmentData
	// pointer to left child
	left *SegmentNode
	// pointer to right child
	right *SegmentNode
}

func (sn *SegmentNode) SegmentDataExist() bool {
	return sn.data != nil
}

func (sn *SegmentNode) GetSegmentData() SegmentData {
	return sn.data
}

func (sn *SegmentNode) SetSegmentData(data SegmentData) {
	sn.data = data
}

func (sn *SegmentNode) get(res Resolver, queryLRange Range, queryRRange Range) (int, int, SegmentData) {
	if queryLRange.IsLesserThanEqualTo(sn.lRange) && queryRRange.IsGreaterThanEqualTo(sn.rRange) {
		if sn.SegmentDataExist() {
			segData := sn.GetSegmentData()
			if segData.IsCached() {
				return 1, 0, sn.GetSegmentData()
			} else {
				return 0, 0, sn.GetSegmentData()
			}
		}
	}

	if queryLRange.Max(sn.lRange).IsGreaterThan(queryRRange.Min(sn.rRange)) {
		return 0, 0, nil
	}

	if sn.lRange.IsEqualTo(sn.rRange) {
		if sn.SegmentDataExist() {
			return 1, 0, sn.GetSegmentData()
		} else {
			// get data from datastore
			rawData := res.Resolve(sn.lRange, sn.rRange)
			segmentData, _ := rawData.Transform()
			segmentData.CacheMe()
			sn.SetSegmentData(segmentData)
			return 0, 1, sn.GetSegmentData()
		}
	}

	if sn.left == nil && queryLRange.IsLesserThan(queryRRange) {
		rRangeForLRange, lRangeForRRang := sn.lRange.SplitInEqualRange(sn.rRange)
		sn.left = NewSegmentNode(sn.lRange, rRangeForLRange)
		sn.right = NewSegmentNode(lRangeForRRang, sn.rRange)
	}

	cacheHitCountLeft, cacheMissCountLeft, leftSegData := sn.left.get(res, queryLRange, queryRRange)
	cacheHitCountRight, cacheMissCountRight, rightSegData := sn.right.get(res, queryLRange, queryRRange)

	totalCacheHitCount := (cacheHitCountLeft + cacheHitCountRight)
	totalCacheMissCount := (cacheMissCountLeft + cacheMissCountRight)
	if leftSegData == nil || rightSegData == nil {
		if leftSegData == nil {
			return cacheHitCountRight, cacheMissCountRight, rightSegData
		} else {
			return cacheHitCountLeft, cacheMissCountLeft, leftSegData
		}
	}

	mergedSegmentData := leftSegData.Merge(rightSegData)
	if queryLRange.IsLesserThanEqualTo(sn.lRange) && queryRRange.IsGreaterThanEqualTo(sn.rRange) {
		mergedSegmentData.CacheMe()
		sn.SetSegmentData(mergedSegmentData)
	}

	return totalCacheHitCount, totalCacheMissCount, mergedSegmentData
}

func NewSegmentTree(totalRange int) *SegmentTree {
	return &SegmentTree{
		totalRange: totalRange,
		numOfNodes: 0,
		root:       nil,
	}
}

type SegmentTree struct {
	// number of elements covered
	totalRange int
	// total number of nodes
	numOfNodes int
	// root of the segment tree
	root *SegmentNode
	// resolver to get data
	res Resolver
}

func (st *SegmentTree) SetRoot(r *SegmentNode) {
	st.root = r
}

func (st *SegmentTree) SetResolver(r Resolver) {
	st.res = r
}

func (st *SegmentTree) Describe() map[string]string {
	description := make(map[string]string)
	description["total range"] = strconv.Itoa(st.totalRange)
	description["number of nodes"] = strconv.Itoa(st.numOfNodes)
	description["root exists"] = fmt.Sprintf("%t", st.root != nil)
	description["resolver exists"] = fmt.Sprintf("%t", st.res != nil)
	return description
}

func (st *SegmentTree) CountNodes() int {
	if st.root == nil {
		return 0
	}

	return st.countNodes(st.root)
}

func (st *SegmentTree) countNodes(node *SegmentNode) int {
	var leftCount, rightCount int
	if node.left != nil {
		leftCount = st.countNodes(node.left)
	}

	if node.right != nil {
		rightCount = st.countNodes(node.right)
	}

	return 1 + leftCount + rightCount
}

func (st *SegmentTree) build() error {
	return nil
}

// given a data source, builds the complete segment tree
func (st *SegmentTree) BuildSegmentTree() error {
	if st.totalRange <= 0 {
		return errors.New("totalRange not set")
	}

	if st.res == nil {
		return errors.New("no resolver")
	}

	st.build()
	return nil
}

func (st *SegmentTree) Get(lQRange Range, rQRange Range) (int, int, SegmentData) {
	if lQRange.IsGreaterThan(rQRange) || st.root == nil {
		return 0, 0, nil
	}

	return st.root.get(st.res, lQRange, rQRange)
}

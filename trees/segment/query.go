package segment

type SegmentQuery interface {
	// left range of the query
	LRange() Range
	// right range of the query
	RRange() Range
	// returns true if caller query is equal to argument query,
	// it excludes range while determining equality
	EqualityWithoutRange(SegmentQuery) bool
}

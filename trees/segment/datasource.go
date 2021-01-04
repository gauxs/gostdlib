package segment

type RawData interface {
	// transforms raw data to segment data
	// you can cache the raw result in say redis and store key
	// in SegmentData during transformation
	Transform() (SegmentData, error)
}

type SegmentData interface {
	// returns merge of caller and argument
	Merge(SegmentData) SegmentData
	// logic to get the data, i.e. it could be a network call etc
	Get() (interface{}, error)
	CacheMe() error
	IsCached() bool
}

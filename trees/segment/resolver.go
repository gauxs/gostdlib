package segment

type Resolver interface {
	// resolves the ranges given from a datasource
	// like array, SQL etc
	Resolve(Range, Range) RawData
}

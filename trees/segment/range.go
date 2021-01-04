package segment

type Range interface {
	// returns min between caller and argument
	Min(Range) Range
	// returns max between caller and argument
	Max(Range) Range
	// returns value of the range
	Value() interface{}
	// first range is rRange for caller,
	// second range is lRange for argument
	SplitInEqualRange(Range) (Range, Range)
	// returns true if caller and argument are equal, else false
	IsEqualTo(Range) bool
	// returns true if caller is < argument, else false
	IsLesserThan(Range) bool
	// returns true if caller is > argument, else false
	IsGreaterThan(Range) bool
	// returns true if caller is <= argument, else false
	IsLesserThanEqualTo(Range) bool
	// returns true if caller is >= argument, else false
	IsGreaterThanEqualTo(Range) bool
	// subtract argument from caller
	Subtract(Range) Range
	// add argument from caller
	Add(Range) Range
	// increment caller by one unit
	Increment() Range
}

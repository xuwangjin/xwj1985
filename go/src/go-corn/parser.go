package parser

import (
	"fmt"
)

const (
	second = iota
	minute
	hour
	day
	month
	week
)

func test() {
	var a = "a"
	fmt.Printf("Average value is: %s", a)
}

var bounds = map[int]struct{ min, max int }{
	second: {0, 59},
	minute: {0, 59},
	hour:   {0, 23},
	day:    {1, 31},
	month:  {1, 12},
	week:   {0, 6},
}

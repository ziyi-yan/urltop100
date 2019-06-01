package main

import (
	"fmt"
	"testing"
)

func Test_partition(t *testing.T) {
	outfiles := partition("/Users/ziyi/misc/urltop100", "/Users/ziyi/misc/urltop100/infile-small.txt", 10)
	fmt.Printf("outfiles: %+v\n", outfiles)
}

func BenchmarkURLTop100(b *testing.B) {
	// b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outfile := top100URL("/Users/ziyi/misc/urltop100", "/Users/ziyi/misc/urltop100/infile.txt", 100*megaByte)
		fmt.Printf("outfile: %s\n", outfile)
	}
}

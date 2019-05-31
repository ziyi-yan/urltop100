package main

import (
	"fmt"
	"testing"
)

func Test_partition(t *testing.T) {
	outfiles := partition("/tmp/urltop100", "/tmp/urltop100/infile.txt", 10)
	fmt.Printf("outfiles: %+v\n", outfiles)
}

func BenchmarkURLTop100(b *testing.B) {
	// b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outfile := top100URL("/tmp/urltop100", "/tmp/urltop100/infile-1G.txt", 10*megaByte)
		fmt.Printf("outfile: %s\n", outfile)
	}
}

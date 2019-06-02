package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func generateFile(nLine int, nCol int) (string, string) {
	buf := bytes.NewBuffer(nil)
	for i := 0; i < nLine; i++ {
		buf.WriteString(randString(rand.Intn(nCol)))
		buf.WriteString("\n")
	}
	dir := os.TempDir()
	f, err := ioutil.TempFile(dir, "")
	if err != nil {
		panic(err)
	}
	buf.WriteTo(f)
	return dir, f.Name()
}

func Test_partition(t *testing.T) {
	dir, originfile := generateFile(100, 100)
	outfiles := partition(dir, originfile, 10)

	out := bytes.NewBuffer(nil)
	for _, f := range outfiles {
		content, err := ioutil.ReadFile(f)
		if err != nil {
			panic(err)
		}
		out.Write(content)
	}

	origin, err := ioutil.ReadFile(originfile)
	if err != nil {
		panic(err)
	}

	if !reflect.DeepEqual(origin, out.Bytes()) { // IMPROVE: use go-cmp
		t.Errorf("contents of output files %+v are not equal to original file %s", outfiles, originfile)
	}
}

func BenchmarkURLTop100(b *testing.B) {
	// b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outfile := top100URL("/Users/ziyi/misc/urltop100", "/Users/ziyi/misc/urltop100/infile.txt", 100*megaByte)
		fmt.Printf("outfile: %s\n", outfile)
	}
}

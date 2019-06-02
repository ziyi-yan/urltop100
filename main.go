package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

// top100URL returns the path to output file containing
// most frequent 100 URLs (with its counts) in input file.
//
// Input file size may be larger than memory size.
// It should use limited memory under the `maxMem`.
func top100URL(dataDir, input string, maxMem int) string {
	mr := GetMRCluster()

	inputSize := fileSize(input)
	nTask := inputSize * mr.NWorkers() / maxMem
	if nTask == 0 {
		nTask = 1
	}
	inputFiles := partition(dataDir, input, nTask)

	countFiles := <-mr.Submit("Count", dataDir, URLCountMap, URLCountReduce, inputFiles, nTask)

	// set nReduce to 1 for final output
	top100Files := <-mr.Submit("Top100", dataDir, URLTop100Map, URLTop100Reduce, countFiles, 1)

	if len(top100Files) != 1 {
		log.Fatalf("number of top100 result files is expected to be one, but got %d", len(top100Files))
	}

	return top100Files[0]
}

// partition returns n files containing
// the content of input file.
func partition(dataDir string, input string, n int) []string {
	f, err := os.Open(input)
	if err != nil {
		log.Fatalf("cannot open file %s", input)
	}

	var partFiles []string
	partSize := fileSize(input) / n
	id := 0
	size := 0
	buf := bytes.NewBuffer(nil)
	buf.Grow(partSize + 2*kiloByte)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		b := sc.Bytes()
		buf.Write(b)
		buf.WriteRune('\n')
		size += len(b)
		if size >= partSize {
			name := partitionName(dataDir, id)
			err := ioutil.WriteFile(name, buf.Bytes(), 0644)
			if err != nil {
				log.Fatalf("cannot write to file %s: %v", name, err)
			}
			partFiles = append(partFiles, name)
			size = 0
			buf.Reset()
			id++
		}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		log.Fatalf("cannot partition file %s: %v", input, err)
	}
	if size > 0 {
		name := partitionName(dataDir, id)
		err := ioutil.WriteFile(name, buf.Bytes(), 0644)
		if err != nil {
			log.Fatalf("cannot write to file %s: %v", name, err)
		}
		partFiles = append(partFiles, name)
	}

	return partFiles
}

func fileSize(file string) int {
	info, err := os.Stat(file)
	if err != nil {
		log.Fatalf("os.Stat(%s) failed: %v", file, err)
	}
	return int(info.Size())
}

func partitionName(dataDir string, id int) string {
	filename := fmt.Sprintf("input-%d.txt", id)
	return filepath.Join(dataDir, filename)
}

func mustCreateFile(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("cannot create file %s: %v", path, err)
	}
	return f
}

func mustWrite(w io.Writer, b []byte) {
	_, err := w.Write(b)
	if err != nil {
		log.Fatalf("cannot write to %v: %v", w, b)
	}
}

func main() {
	outfile := top100URL("/tmp/urltop100", "/tmp/urltop100/infile-1G.txt", 10*megaByte)
	fmt.Printf("outfile: %s\n", outfile)
}

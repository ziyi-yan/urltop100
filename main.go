package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
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
		panic(err)
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

var dataDir = flag.String("dataDir", "", "path to data directory")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// ... rest of the program ...
	dir := *dataDir
	input := filepath.Join(dir, "input")
	inputSize := fileSize(input)
	maxMem := inputSize / 100

	fmt.Printf("input:\t%s\nsize:\t%d\nmaxMem:\t%d\n", input, inputSize, maxMem)

	start := time.Now()

	outfile := top100URL(dir, input, maxMem)

	end := time.Now()
	fmt.Printf("duration: %v\n", end.Sub(start))
	fmt.Printf("outfile: %s\n", outfile)
	// program end

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

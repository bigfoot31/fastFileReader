// copyright 2020 Probhonjon Baruah ( github.com/bigfoot31 ).

// Package filereader implements asynchronous reading of a file
// also a time comparision to synchronous reading of file is done.
package filereader

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

// chunk size that each asynchronous thread will read
// currently set to 1MB
const asyncChunkSize = 1024 * 1024

// buffer size of the synchronous scanner
// the size of each line is too big for the default buffer size of 64kB
// increased buffer size to 512kB
const syncBufferSize = 512 * 1024

var wg sync.WaitGroup

func main() {
	// command line args
	filename := flag.String("f", "", "path to file")

	flag.Parse()

	// throw fatal error if file path not passed in cmd line
	if *filename == "" {
		log.Fatal("filename is empty")
		return
	}

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatal("cannot able to read the file", err)
		return
	}
	defer file.Close()

	startTime := time.Now()
	syncReadFile(file)
	endTime := time.Now()
	log.Println("time taken for syncronous file reading", endTime.Sub(startTime))

	startTime = time.Now()
	asyncReadFile(file)
	endTime = time.Now()
	log.Println("time taken for asyncronous file reading", endTime.Sub(startTime))
}

func asyncReadFile(file *os.File) {
	fileStats, e := file.Stat()
	if e != nil {
		log.Println(e)
		return
	}

	filesize := int(fileStats.Size())

	// Number of go routines we need to spawn.
	concurrency := filesize / asyncChunkSize
	// check for any left over bytes. Add one more go routine if required.
	if filesize%asyncChunkSize != 0 {
		concurrency++
	}

	// create an array of same size as number of goroutines
	// each element of the array indicates the offset at which
	// that goroutine should start reading the file
	chunkOffset := make([]int64, concurrency)

	// Offsets depend on the index.
	// Second go routine should start at 100, for example, given a
	// buffer size of 100.
	for i := 0; i < concurrency; i++ {
		chunkOffset[i] = int64(asyncChunkSize * i)
	}

	// get number of cpu in the current machine
	gochannel := make(chan int64, runtime.NumCPU())

	// run as many jobs as the number of cpus available
	// once a job is completed, initiate the next job
	// The number of jobs running at any one time should
	// be same as number of cpu.
	//
	// This is achieved using a integer gochannel of size = #cpu
	// Once a async read job starts, add a int to the goChannel.
	// When the go channel is full, the for loop suspends till a
	// space becomes available in the channel.
	// When the job is completed the async function ejects a element
	// from the channel, leaving a empty space in the channel for
	// the next job to start.
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		gochannel <- 1
		go readChunk(gochannel, file, chunkOffset, i)
	}

	wg.Wait()
}

func readChunk(gochannel chan int64, file *os.File, chunkOffset []int64, i int) {
	// read certain bytes from the file starting at offset
	_, _err := file.ReadAt(make([]byte, asyncChunkSize), chunkOffset[i])

	if _err != nil && _err != io.EOF {
		log.Println(_err)
		return
	}

	<-gochannel
	wg.Done()
}

func syncReadFile(file *os.File) {
	scanner := bufio.NewScanner(file)

	// increase buffer size of scanner
	buf := make([]byte, syncBufferSize)
	scanner.Buffer(buf, syncBufferSize)
	for scanner.Scan() {
		_ = scanner.Text()
	}
}

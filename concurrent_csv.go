package concurrent_csv

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var NumOfRequestedRTs int
var ProcessDir string
var CSVFilename string
var RequiredCSVColumnHeaders []string
var ErrorCSVColumnHeaders []string

// ExecRoutinesRecordSets divides csv recrods into batches based on number of Go routines requested
// passes the batches of records into function passed into it, and executes it on the Go routines
// returns number of Go routines actually created.
func ExecRoutinesRecordSets(
	recordSetLogic func(rtNum int, headerRow []string, dataRowsHeap [][]string, h map[string]int),
) {

	if ProcessDir == "" {
		log.Fatalln("Must set the concurrent_csv.ProcessDir.")
	}

	if CSVFilename == "" {
		log.Fatalln("Must set the concurrent_csv.CSVFilename.")
	}

	if RequiredCSVColumnHeaders == nil {
		log.Fatalln("Must set the concurrent_csv.RequiredCSVColumnHeaders.")
	}

	if ErrorCSVColumnHeaders == nil {
		log.Fatalln("Must set the concurrent_csv.ErrorCSVColumnHeaders.")
	}

	csvPath := ProcessDir + "/in/" + CSVFilename

	allConcurrentRecordsStartTime := time.Now().Unix()

	WriteToLog("Start All Records: \n\n", 0, false)

	csvfile, err := os.Open(csvPath)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	totalRowCount := countCSVRows(csvfile)

	if totalRowCount < NumOfRequestedRTs {
		NumOfRequestedRTs = totalRowCount
	}

	var recordsPerRT int = totalRowCount / NumOfRequestedRTs

	r := csv.NewReader(csvfile)

	firstCsvLine := true
	headerRow := []string{}
	dataRowsHeap := [][]string{}
	h := make(map[string]int)

	rtNum := 1
	rowNum := 1

	var wg sync.WaitGroup
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if firstCsvLine {

			for x := 0; x < len(record); x++ {
				h[record[x]] = x
			}

			// MAKE SURE ALL DEPENDANT COLUMNS EXIST BEFORE PROCEEDING
			for _, columnName := range RequiredCSVColumnHeaders {
				if _, ok := h[columnName]; !ok {
					panic("\"" + columnName + "\" column header missing from csv.")
				}
			}

			headerRow = append(headerRow, record...)

		} else if !firstCsvLine {

			dataRowsHeap = append(dataRowsHeap, record)

			if rowNum%recordsPerRT == 0 && rtNum < NumOfRequestedRTs {

				wg.Add(1)
				go func(rtNum int, headerRow []string, dataRowsHeap [][]string) {

					defer wg.Done()

					// Execute the custom method from outside the package, on the go routine
					recordSetLogic(rtNum, headerRow, dataRowsHeap, h)
				}(rtNum, headerRow, dataRowsHeap)
				dataRowsHeap = [][]string{}
				rtNum++

			}
			rowNum++

		}

		firstCsvLine = false

	}

	// If there are extra records, tack them onto the last csv.
	if len(dataRowsHeap) > 0 {

		wg.Add(1)
		go func(rtNum int, headerRow []string, dataRowsHeap [][]string) {

			defer wg.Done()

			// Execute the custom method from outside the package, on the go routine
			recordSetLogic(rtNum, headerRow, dataRowsHeap, h)
		}(rtNum, headerRow, dataRowsHeap)
	}

	wg.Wait()
	csvfile.Close()

	aggregateSuccessLogCSVs(rtNum)
	aggregateErrorLogCSVs(rtNum)

	// log how long all the go routines took altogether
	allConcurrentRecordsCompleteTime := int(time.Now().Unix() - allConcurrentRecordsStartTime)
	WriteToLog("Finished All Concurrent Records \nTotal Execution Time: "+strconv.Itoa(allConcurrentRecordsCompleteTime)+" second(s) \n\n\n\n", 0, true)

}

func countCSVRows(csvfile *os.File) int {

	r := csv.NewReader(csvfile)

	firstCsvLine := true
	rowCount := 0

	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if !firstCsvLine {
			rowCount++
		}

		firstCsvLine = false
	}

	// shift the cursor back to starting position on the file
	csvfile.Seek(0, io.SeekStart)

	return rowCount

}

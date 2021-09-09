package concurrent_csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

// ^_^_^_^_^_^_^_^ GENERAL LOGGING ^_^_^_^_^_^_^_^ //

func WriteToLog(output string, rtNum int, append bool) {
	// If the file doesn't exist, create it, or append to the file
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	f, err := os.OpenFile(ProcessDir+"/logs/"+strconv.Itoa(rtNum)+"-log.txt", writeOptions, 0644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte(output)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

// ^_^_^_^_^_^_^_^ SUCCESS LOGGING ^_^_^_^_^_^_^_^ //

func WriteToSuccessLogCSV(dataRow []string, csvNum int, append bool) {

	csvDataRows := [][]string{dataRow}
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	f, err := os.OpenFile(ProcessDir+"/out/"+strconv.Itoa(csvNum)+".csv", writeOptions, 0644)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer f.Close()

	w := csv.NewWriter(f)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}

}

func aggregateSuccessLogCSVs(numCSVs int) {

	//Empty csv if it exists already
	writeToAggregatedSuccessLogCSV([][]string{}, false)

	firstCSV := true

	for csvNum := 1; csvNum <= numCSVs; csvNum++ {

		func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
			defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
				if ex := recover(); ex != nil {
					fmt.Println(fmt.Errorf("%v", ex))
					// WriteToLog("\n\n\nException thrown: "+err.Error()+"\n\n\n", csvNum, true, "errors")
				}
			}()

			csvfile, err := os.Open(ProcessDir + "/out/" + strconv.Itoa(csvNum) + ".csv")
			if err != nil {
				log.Fatalln("Couldn't open the csv file", err)
			}

			r := csv.NewReader(csvfile)

			dataRowsHeap := [][]string{}

			firstCsvLine := true
			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
					defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
						if ex := recover(); ex != nil {
							fmt.Println(fmt.Errorf("%v", ex))
						}
					}()

					if firstCsvLine && firstCSV { // first line is just header names
						dataRowsHeap = append(dataRowsHeap, record)
					} else if !firstCsvLine {
						dataRowsHeap = append(dataRowsHeap, record)
					}
				}()
				firstCsvLine = false
			}

			writeToAggregatedSuccessLogCSV(dataRowsHeap, true)

			csvfile.Close()

			// after scraping it, delete it.
			var errDel = os.Remove(ProcessDir + "/out/" + strconv.Itoa(csvNum) + ".csv")
			if errDel != nil {
				fmt.Println(errDel.Error())
			}
		}()
		firstCSV = false
	}

}

// writeToAggregatedSuccessLogCSV() Takes matrix of records and writes them to bundled version of csv error logs
/// used only by AggregateSuccessLogCSVs
func writeToAggregatedSuccessLogCSV(csvDataRows [][]string, append bool) {

	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(ProcessDir+"/out/"+CSVFilename, writeOptions, 0644)

	if err != nil {
		os.Exit(1)
	}

	defer file.Close()

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

// ^_^_^_^_^_^_^_^ ERROR LOGGING ^_^_^_^_^_^_^_^ //

func WriteToErrorLogCSV(dataRow []string, rtNum int, append bool) {

	csvDataRows := [][]string{dataRow}
	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(ProcessDir+"/logs/"+strconv.Itoa(rtNum)+"-errors.csv", writeOptions, 0644)

	if err != nil {
		os.Exit(1)
	}

	defer file.Close()

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}

}

func aggregateErrorLogCSVs(numCSVs int) {

	//Empty csv if it exists already
	writeToAggregatedErrorCSV([][]string{ErrorCSVColumnHeaders}, false)

	for csvNum := 1; csvNum <= numCSVs; csvNum++ {

		func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
			defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
				if ex := recover(); ex != nil {
					// Silent fail
					//fmt.Println(fmt.Errorf("%v", ex))
				}
			}()

			csvfile, err := os.Open(ProcessDir + "/logs/" + strconv.Itoa(csvNum) + "-errors.csv")
			if err != nil {
				panic(err)
			}

			r := csv.NewReader(csvfile)

			dataRowsHeap := [][]string{}

			for {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				func() { // anonymous function, so we can do a controlled "try, catch" or panic recover as Go calls it.
					defer func() { // this is the "catch". It runs at the end of it's enclosing function if there is an "exception"/panic thrown.
						if ex := recover(); ex != nil {
							fmt.Println(fmt.Errorf("%v", ex))
						}
					}()

					dataRowsHeap = append(dataRowsHeap, record)

				}()

			}

			writeToAggregatedErrorCSV(dataRowsHeap, true)

			csvfile.Close()

			// after scraping it, delete it.
			var errDel = os.Remove(ProcessDir + "/logs/" + strconv.Itoa(csvNum) + "-errors.csv")
			if errDel != nil {
				fmt.Println(errDel.Error())
			}
		}()

	}

}

// writeToAggregatedErrorCSV() Takes matrix of records and writes them to bundled version of csv error logs
/// used only by AggregateErrorLogCSVs
func writeToAggregatedErrorCSV(csvDataRows [][]string, append bool) {

	writeOptions := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if append {
		writeOptions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(ProcessDir+"/logs/output-errors.csv", writeOptions, 0644)

	if err != nil {
		os.Exit(1)
	}

	defer file.Close()

	w := csv.NewWriter(file)
	w.WriteAll(csvDataRows)
	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}

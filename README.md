# concurrent_csv

To use this package:

- Create a directory structure similar to the following:
```
    myProjectFolder/ 

    myProjectFolder/in/ (this is the csv folder containing your csv to be processed) 

    myProjectFolder/logs/ 

    myProjectFolder/out/ 

    myProjectFolder/main.go (this is the go file that will import the concurrent_csv package) 
```
- Place your csv to be processed in the /in/ folder. eg:
myProjectFolder/in/all-records.csv
- Example usage in main.go:
```
type MyJSONStruct struct {
	SomeDate  string
	Amount float64
}

// absolutePathToThisFilesFolder() gets absolute path to this files parent folder as a string.
// has to be run directly in the file you wish to get the path for, hence why
// it's not in a package.
func absolutePathToThisFilesFolder() string {
	_, thisFilesLocation, _, _ := runtime.Caller(0)
	return strings.Split(thisFilesLocation, "/main.go")[0]
}

func main() {

	concurrent_csv.NumOfRequestedRTs, _ = strconv.Atoi(os.Args[1])

	concurrent_csv.ProcessDir = absolutePathToThisFilesFolder()
	concurrent_csv.CSVFilename = "all-records.csv"
	concurrent_csv.RequiredCSVColumnHeaders = []string{"RecordID"}
	concurrent_csv.ErrorCSVColumnHeaders = []string{ // example error tracking setup for api call for each record
		"rtNum", // this would be the go routine number
		"RecordID",
		"method",
		"Endpoint",
		"Http_Response_Code",
		"Request_Body",
		"Response_Body",
		"Exception_Message",
		"DateTime_Stamp",
	}

	concurrent_csv.ExecRoutinesRecordSets(recordSetLogic) // pass in function that has to have specific signature

}

// recordSetLogic
// the required function signature for concurrent_csv.ExecRoutinesRecordSets is used on this function
func recordSetLogic(rtNum int, headerRow []string, dataRowsHeap [][]string, h map[string]int) {

	concurrent_csv.WriteToLog("Start All Records from Routine #"+strconv.Itoa(rtNum)+": \n\n", rtNum, false)

	concurrent_csv.WriteToSuccessLogCSV(headerRow, rtNum, false)

	// READING RECORDS.
	for _, dataRow := range dataRowsHeap {
		func() {
			defer func() {
				if ex := recover(); ex != nil {
					err := fmt.Errorf("%v", ex)
					fmt.Println(err.Error())
				}
			}()

			recordID := dataRow[h["RecordID"]]

			// API CALL
			var myJSONStruct MyJSONStruct             // create null var
			doExternalAPICall(recordID, &myJSONStruct, rtNum) // set null var by pointer

			// Now loop through fees to add on the feeAmount to paymentTotalsMap
			if myJSONStruct.Amount > 5.00 {
			    // do something custom
			}

			// logging success
			// if we get to these lines, we got a 200 range response and assume success.
			// The panics on failure in the API call above would skip these lines below otherwise
			concurrent_csv.WriteToLog("\tdoExternalAPICall api call successful for "+recordID+"\n", rtNum, true)

            concurrent_csv.WriteToSuccessLogCSV(dataRow, rtNum, true)


		}()
	}

	concurrent_csv.WriteToLog("Finished All Records for "+strconv.Itoa(rtNum)+".csv \n\n\n\n", rtNum, true)

}

func doExternalAPICall(recordID string, myJSONStruct *MyJSONStruct, rtNum int) {

	// Set api endpoint
	url := os.Getenv("DOMAIN_ENV") + "/some-endpoint/" + recordID

	// Set Request Method
	req, _ := http.NewRequest("GET", url, nil)

	// Set Request Headers
	req.Header.Set("Content-Type", "application/json")

	// Execute API Request
	client := &http.Client{}
	resp, err := client.Do(req)

	// Log Results
	currentDateTimeStamp := time.Now().In("America/Los_Angeles").Format("2006-01-02 3:04:05 PM")
	if err != nil {

		dataRow := []string{
			strconv.Itoa(rtNum),
			recordID,
			"doExternalAPICall()",
			url,
			resp.Status,
			"",
			"",
			err.Error(),
			currentDateTimeStamp,
		}

		concurrent_csv.WriteToErrorLogCSV(dataRow, rtNum, true)

		panic("API Exception Error: " + err.Error())

	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {

		responseBody, _ := ioutil.ReadAll(resp.Body)

		dataRow := []string{
			strconv.Itoa(rtNum),
			recordID,
			"doExternalAPICall()",
			url,
			resp.Status,
			"",
			string(responseBody),
			"",
			currentDateTimeStamp,
		}

		concurrent_csv.WriteToErrorLogCSV(dataRow, rtNum, true)
		panic(resp.Status)

	}

	respBody, _ := ioutil.ReadAll(resp.Body)

	json.Unmarshal(respBody, myJSONStruct) // set null variable using pointer

}
```
- To execute the go process, invoke the main.go file with the `go run` command followed by the number of go routines
you would like executed to process the csv records concurrently for:
```
$ go run myProjectFolder/main.go 7
```
- Note that the number you pass you will cause the process to divide, as evenly as possible, all the csv rows across the number of routines you requested based on the number you pass in through the command line. Most edge cases should be handled automatically by the package, such as if you were to pass the number 7 but only 6 rows exist in your csv, the process would change the number of go routines to 6 (so one csv row per routine). If you pass in a non divisible number for the number of rows in your csv (example: 4 concurrency for 10 rows), it gets the closest evenly divisible amount , spreads them evenly across all routines, and the remainder all go on the last routine (so in the example, 2 records for each of the 4 routines, and the remainder extra 2 go on the 4th routine, making the 4th routine process 4 rows intead of 2).
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	tableName       = "dynamo-table-name"
	region          = "us-west-2"
	numberOfRecords = 400
)

func scan(dbScanner *DynamoScanner, process func(item DynamoItem)) {
	start := time.Now()
	err := dbScanner.ScanItems(process)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Normal execution took %s\n", elapsed)
}

func concurrentScan(dbScanner *DynamoScanner, process func(item DynamoItem)) {
	start := time.Now()
	done, err := dbScanner.ConcurrentScanItems(process)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Printf("Concurrent execution took %s\n", elapsed)
	<-done
	close(done)
}

func main() {

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	fmt.Printf("Starting ... \n")
	fmt.Printf(" numberOfRecors: %v\n", numberOfRecords)
	fmt.Printf(" tableName: %v\n", tableName)
	fmt.Printf(" region: %v\n", region)
	fmt.Printf(" recordsInChannel: %v\n", recordsInChannel)

	f, err := os.Create("_data.text")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer f.Close()

	process := func(item DynamoItem) {
		id, ok := item["Id"]
		if ok {
			_, err := f.WriteString(fmt.Sprintln(id))
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}

	dbScanner := NewDynamoScanner(dynamodb.New(sess), tableName)
	// err := dbScanner.CreateRandomItems(client, numberOfRecords)

	f.WriteString("************************************************************************************************\n")
	concurrentScan(dbScanner, process)
	time.Sleep(100 * time.Millisecond)

	f.WriteString("************************************************************************************************\n")
	scan(dbScanner, process)
	return
}

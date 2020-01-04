package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
)

const (
	tableName       = "dynamo-table-name"
	region          = "us-west-2"
	numberOfRecords = 400
)

func createItem(id uuid.UUID, created time.Time, title, name, description string) (map[string]*dynamodb.AttributeValue, error) {
	number := rand.Int()
	item := map[string]Any{
		"Title":       fmt.Sprintf("%v_%v", title, number),
		"Name":        fmt.Sprintf("%v_%v", name, number),
		"Description": description + randomString(50),
		"Id":          id.String(),
		"Created":     created.Format(time.RFC3339Nano),
		"Data":        randomString(5000),
	}
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return av, nil
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	digits := "0123456789"
	specials := "~=+%^*/()[]{}/!@#$?|"
	all := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		digits + specials
	buf := make([]byte, length)
	buf[0] = digits[rand.Intn(len(digits))]
	buf[1] = specials[rand.Intn(len(specials))]
	for i := 2; i < length; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	str := string(buf)
	return str
}

func createRandomItems(client *dynamodb.DynamoDB, n int) error {
	for i := 0; i < n; i++ {
		item, err := createItem(uuid.New(), time.Now().UTC(), "title", "name", "key: ")
		if err != nil {
			return err
		}
		params := &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		}
		req, _ := client.PutItemRequest(params)
		err = req.Send()

		if err != nil {
			return err
		}
	}
	return nil
}

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
	// err := createRandomItems(client, numberOfRecords)

	f.WriteString("************************************************************************************************\n")
	concurrentScan(dbScanner, process)
	time.Sleep(100 * time.Millisecond)

	f.WriteString("************************************************************************************************\n")
	scan(dbScanner, process)
	return
}

package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	recordsInChannel = 200
)

// Any ...
type Any interface{}

// DynamoItem ...
type DynamoItem map[string]Any

//DynamoScanner ...
type DynamoScanner struct {
	tableName *string
	data      chan map[string]Any
	done      chan bool
	client    dynamodbiface.DynamoDBAPI
}

//NewDynamoScanner ...
func NewDynamoScanner(client dynamodbiface.DynamoDBAPI, tableName string) *DynamoScanner {
	return &DynamoScanner{
		tableName: aws.String(tableName),
		data:      make(chan map[string]Any, recordsInChannel),
		done:      make(chan bool),
		client:    client,
	}
}

func getItem(page map[string]*dynamodb.AttributeValue) (DynamoItem, error) {
	item := DynamoItem{}
	err := dynamodbattribute.UnmarshalMap(page, &item)
	return item, err
}

//ScanItems ...
func (db *DynamoScanner) ScanItems(process func(DynamoItem)) error {
	if process == nil {
		return errors.New("Missing input argument")
	}
	handlePage := func(page *dynamodb.ScanOutput, lastPage bool) bool {
		for _, p := range page.Items {
			item, err := getItem(p)
			if err != nil {
				fmt.Println("Got error unmarshalling: " + err.Error())
			} else {
				process(item)
			}
		}
		return !lastPage
	}

	params := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		TableName:      db.tableName,
	}
	err := db.client.ScanPages(params, handlePage)
	return err
}

//ConcurrentScanItems ...
func (db *DynamoScanner) ConcurrentScanItems(process func(DynamoItem)) (chan bool, error) {
	concurrentHandlePage := func(page *dynamodb.ScanOutput, lastPage bool) bool {
		for _, p := range page.Items {
			item, err := getItem(p)
			if err != nil {
				fmt.Println("Got error unmarshalling: " + err.Error())
			} else {
				db.data <- item
			}
		}
		if lastPage {
			close(db.data)
		}
		return !lastPage
	}
	go func() {
		for {
			item, more := <-db.data
			if more {
				process(item)
			} else {
				db.done <- true
				return
			}
		}
	}()
	params := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		TableName:      db.tableName,
	}
	err := db.client.ScanPages(params, concurrentHandlePage)
	return db.done, err
}

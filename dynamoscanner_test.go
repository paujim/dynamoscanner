package main

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (db *mockedDynamoScanner) ScanPages(input *dynamodb.ScanInput, handler func(*dynamodb.ScanOutput, bool) bool) error {
	ouput := db.output["*dynamodb.ScanOutput"].(*dynamodb.ScanOutput)
	lastPage := db.output["bool"].(bool)
	handler(ouput, lastPage)
	return db.err
}

func TestSuccessScanItems(t *testing.T) {

	values := []map[string]*dynamodb.AttributeValue{
		map[string]*dynamodb.AttributeValue{"Id": &dynamodb.AttributeValue{S: aws.String("STRING")}},
	}

	output := &dynamodb.ScanOutput{
		Count: aws.Int64(1),
		Items: values,
	}
	mockClient := &mockedDynamoScanner{}
	mockClient.With(output).With(true).WithError(nil)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	total := 0
	err := scanner.ScanItems(func(DynamoItem) {
		total++
	})
	if err != nil {
		t.Errorf("Error: %v\n", err)
	}
	if total != 1 {
		t.Errorf("Expected one")
	}
}

func TestFailScanItems(t *testing.T) {
	expectedError := errors.New("Something")
	mockClient := &mockedDynamoScanner{}
	mockClient.
		With(&dynamodb.ScanOutput{}).
		With(true).
		WithError(expectedError)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	err := scanner.ScanItems(func(DynamoItem) {})
	if err != expectedError {
		t.Errorf("Expected: %v\n", expectedError)
	}
}

func TestFailScanItemsWithNoPageHandler(t *testing.T) {
	mockClient := &mockedDynamoScanner{}
	mockClient.WithError(nil)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	err := scanner.ScanItems(nil)
	if err == nil {
		t.Errorf("Expected error\n")
	}
}

func TestConcurrentScanItems(t *testing.T) {
	values := []map[string]*dynamodb.AttributeValue{
		map[string]*dynamodb.AttributeValue{"Id": &dynamodb.AttributeValue{S: aws.String("STRING")}},
	}

	output := &dynamodb.ScanOutput{
		Count: aws.Int64(1),
		Items: values,
	}
	mockClient := &mockedDynamoScanner{}
	mockClient.With(output).With(true).WithError(nil)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	total := 0
	done, err := scanner.ConcurrentScanItems(func(DynamoItem) {
		total++
	})
	if err != nil {
		t.Errorf("Error: %v\n", err)
	}
	if <-done != true {
		t.Errorf("Expected true")
	}
	if total != 1 {
		t.Errorf("Expected one")
	}
}

func TestFailConcurrentScanItems(t *testing.T) {
	expectedError := errors.New("Something")
	mockClient := &mockedDynamoScanner{}
	mockClient.With(&dynamodb.ScanOutput{}).With(true).WithError(expectedError)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	_, err := scanner.ConcurrentScanItems(func(DynamoItem) {})
	if err != expectedError {
		t.Errorf("Error: %v\n", expectedError)
	}
}

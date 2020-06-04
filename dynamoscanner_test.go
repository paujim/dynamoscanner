package main

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func (db *mockedDynamoScanner) ScanPages(input *dynamodb.ScanInput, handler func(*dynamodb.ScanOutput, bool) bool) error {
	output := db.output["*dynamodb.ScanOutput"].(*dynamodb.ScanOutput)
	lastPage := db.output["bool"].(bool)
	handler(output, lastPage)
	return db.err
}

func TestSuccessScanItems(t *testing.T) {

	values := []map[string]*dynamodb.AttributeValue{
		{"Id": &dynamodb.AttributeValue{S: aws.String("STRING")}},
	}

	output := &dynamodb.ScanOutput{
		Count: aws.Int64(1),
		Items: values,
	}
	mockClient := &mockedDynamoScanner{}
	mockClient.With(output).With(true).WithError(nil)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	total := 0
	assert.NoError(t, scanner.ScanItems(func(DynamoItem) {
		total++
	}), "Scan Items Error")
	assert.Equal(t, 1, total, "Expected one")
}

func TestFailScanItems(t *testing.T) {
	expectedError := errors.New("Something")
	mockClient := &mockedDynamoScanner{}
	mockClient.
		With(&dynamodb.ScanOutput{}).
		With(true).
		WithError(expectedError)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	assert.Equal(t, scanner.ScanItems(func(DynamoItem) {}), expectedError, "Expected Error [%s]", expectedError)
}

func TestFailScanItemsWithNoPageHandler(t *testing.T) {
	mockClient := &mockedDynamoScanner{}
	mockClient.WithError(nil)

	scanner := NewDynamoScanner(mockClient, "TestTable")
	assert.Error(t, scanner.ScanItems(nil), "Expected error when nil argument")

}

func TestConcurrentScanItems(t *testing.T) {
	values := []map[string]*dynamodb.AttributeValue{
		{"Id": &dynamodb.AttributeValue{S: aws.String("STRING")}},
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

	assert.NoError(t, err, "Error: %v\n", err)
	assert.True(t, <-done, "Expected true")
	assert.Equal(t, 1, total, "Expected one")

}

func TestFailConcurrentScanItems(t *testing.T) {
	expectedError := errors.New("Something")
	mockClient := &mockedDynamoScanner{}
	mockClient.With(&dynamodb.ScanOutput{}).With(true).WithError(expectedError)

	scanner := NewDynamoScanner(mockClient, "TestTable")

	_, err := scanner.ConcurrentScanItems(func(DynamoItem) {})
	assert.EqualError(t, err, expectedError.Error(), "Error: %v\n", expectedError)

}

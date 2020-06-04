package main

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func (db *mockedDynamoScanner) PutItemRequest(input *dynamodb.PutItemInput) (*request.Request, *dynamodb.PutItemOutput) {
	req := db.output["*request.Request"].(*request.Request)
	output := db.output["*dynamodb.PutItemOutput"].(*dynamodb.PutItemOutput)
	return req, output
}

func TestCreateRandomItems(t *testing.T) {
	mockClient := &mockedDynamoScanner{}
	mockClient.With(&request.Request{}).With(&dynamodb.PutItemOutput{})
	scanner := NewDynamoScanner(mockClient, "TestTable")
	assert.NoError(t, scanner.CreateRandomItems(10), "Go an error")
}

func TestFailCreateRandomItems(t *testing.T) {
	expectedError := errors.New("something argument")
	mockClient := &mockedDynamoScanner{}
	mockClient.With(&request.Request{Error: expectedError}).With(&dynamodb.PutItemOutput{})
	scanner := NewDynamoScanner(mockClient, "TestTable")
	assert.EqualError(t, scanner.CreateRandomItems(10), expectedError.Error())
}

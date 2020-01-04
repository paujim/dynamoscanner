package main

import (
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockedDynamoScanner struct {
	dynamodbiface.DynamoDBAPI
	output map[string]Any
	err    error
}

func (db *mockedDynamoScanner) With(obj Any) *mockedDynamoScanner {
	key := reflect.TypeOf(obj).String()
	if db.output == nil {
		db.output = map[string]Any{}
	}
	db.output[key] = obj
	return db
}
func (db *mockedDynamoScanner) WithError(err error) *mockedDynamoScanner {
	db.err = err
	return db
}

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
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

// CreateRandomItems ...
func (db *DynamoScanner) CreateRandomItems(n int) error {
	for i := 0; i < n; i++ {
		item, err := createItem(uuid.New(), time.Now().UTC(), "title", "name", "key: ")
		if err != nil {
			return err
		}
		params := &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		}
		req, _ := db.client.PutItemRequest(params)
		err = req.Send()

		if err != nil {
			return err
		}
	}
	return nil
}

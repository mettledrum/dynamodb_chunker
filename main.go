package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dynamodb_chunker/chunker"
)

func main() {
	svc := dynamodb.New(session.New(), aws.NewConfig().WithRegion("us-east-2"))

	type thing struct {
		ID       int64  `json:"id"`
		Contents string `json:"contents"`
	}

	t := thing{
		ID:       123,
		Contents: "what's up doc?",
	}

	err := chunker.Upsert(t.ID, t, svc)
	fmt.Println(err)

	b, err := chunker.Get(t.ID, svc)
	fmt.Println(b, err)
}

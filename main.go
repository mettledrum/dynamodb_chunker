package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/dynamodb_chunker/chunker"
)

func main() {
	c := &aws.Config{Region: aws.String("us-east-2"),
		Credentials: credentials.NewStaticCredentials("", "", ""),
	}
	svc := dynamodb.New(session.New(c))

	type thing struct {
		ID       int64  `json:"id"`
		Contents string `json:"contents"`
	}

	t := thing{
		ID:       123,
		Contents: "12345678",
	}

	err := chunker.Upsert(t.ID, t, svc)
	fmt.Println(err)

	t2 := thing{}
	err = chunker.Get(t.ID, svc, &t2)
	fmt.Println(t2, err)
}

package chunker

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

func toItemAndChunks(id int64, x interface{}) itemMap {
	b, err := json.Marshal(&x)
	if err != nil {
		panic(err)
	}

	var z bytes.Buffer
	w := gzip.NewWriter(&z)
	w.Write(b)
	w.Close()

	u := uuid.NewUUID()
	cs := getChunks(z, u, id)
	m := itemMap{
		ID:             id,
		CurrentVersion: u,
		Chunks:         cs,
		ChunkCount:     len(cs),
		UpdateTime:     time.Now().UnixNano(),
	}

	return m
}

func getChunkIDs(id int64, uuid string, cnt int64) string {
	return fmt.Sprintf("%d•%s•%d", id, uuid, cnt)
}

func getChunks(z []byte, cv string, id int64) []itemChunk {
	chunks := []itemChunk{}
	chunkCount := 0
	for i := 0; i < len(z); i += maxItemLen {
		chunkCount++

		end := i + maxItemLen
		if end > len(z) {
			end = len(z)
		}

		c := itemChunk{
			ID:   getChunkIDs(id, cv, chunkCount),
			Body: z[i:end],
		}

		chunks = append(chunks, c)
	}

	return chunks
}

type jsonTime time.Time

func (j jsonTime) MarshalJSON() ([]byte, error) {
	s := j.UTC().Format(time.RFC3339)
	return []byte(s), nil
}

type itemMap struct {
	ID         int64       // matches ID of resource we're chunking (PK)
	UpdateTime jsonTime    // the latest UpdateTime "wins" when writing to itemMap
	ChunkCount int64       // number of chunks resource has been broken up into
	Chunks     []itemChunk // the chunks that comprise the item when concatenated
	UUID       string      // prevents case where UpdateTime is same for requests
}

// TODO: bytes or string length
const maxItemLen = 100 // 400kbps in DynamoDB

type itemChunk struct {
	ID   string // "ID•UUID•ChunkIdx" (PK)
	Body []byte // gzipped binary of resource contents
}

// Get retrieves the item and its concatenated chunks stored in DynamoDB
func Get(id int64) ([]byte, error) {
	svc := dynamodb.New(session.New())

	// get ItemMap to get ItemChunk IDs
	getParams := &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.Itoa(id)), // N is a *string
			},
		},
		TableName: "ItemMap",
	}
	getRes, err := svc.GetItem(getParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get ItemMap response: %v\n", getRes)

	uuid := getRes.Item["UUID"].String()
	updateTime := getRes.Item["UpdateTime"].String()
	chunkCount := getRes.Item["ChunkCount"].String()

	chunkKeys := []map[string]*dynamodb.AttributeValue{}
	for i := 0; i < chunkCount; i++ {
		chunkID := append(chunkIDs, getChunkIDs(id, uuid, i))
		k := map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(chunkID),
			},
		}
		chunkKeys = append(chunkKeys, k)
	}

	getChunkParams := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			"ItemChunks": {
				Keys: chunkKeys,
			},
		},
	}

	getChunkRes, err := svc.BatchGetItem(getChunkParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get ItemChunks response: %v\n", getChunkRes)

	return json.Marshal(getChunkRes)
}

// Upsert creates or updates a resource in multiple tables and items in DynamoDB
// 1) BatchWriteItem([]itemChunk)
// 2) PutItem(itemMap) conditionally if UpdateTime of request is newer
func Upsert(id int64, x interface{}) error {
	svc := dynamodb.New(session.New())

	m := toItemAndChunks(id, x)

	// (1)
	ws := []*dynamodb.WriteRequest{}
	for _, c := range m.Chunks {
		w := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(c.ID),
					},
					"Body": {
						B: aws.String(c.Body), // automatically base64 encoded
					},
				},
			},
		}
		ws = append(ws, w)
	}
	batchParams := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			"ItemChunks": ws,
		},
	}
	batchRes, err := svc.BatchWriteItem(batchParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("response from batch chunk write: %v\n", batchRes)

	// (2)
	putParams := &dynamodb.PutItemInput{
		UpdateExpression: "SET #ut = :ut, #cc = :cc, #uuid = :uuid IF #ut < :ut",
		ExpressionAttributeNames: {
			"#ut":   aws.String("UpdateTime"),
			"#cc":   aws.String("ChunkCount"),
			"#uuid": aws.String("UUID"),
		},
		ExpressionAttributeValues: {
			":ut": {
				N: aws.String(m),
			},
			":cc": {
				N: aws.String(strconv.Itoa(m.ChunkCount)),
			},
			":uuid": {
				N: aws.String(strconv.Itoa(m.UUID)),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.Itoa(m.ID)),
			},
		},
	}

	putRes, err = svc.PutItem(putParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("response from item put: %v\n", putRes)

	return nil
}

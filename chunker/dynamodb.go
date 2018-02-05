package chunker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

// pre-reqs
// table of "ItemMap" with PK of "ID" of type Number
// table of "ItemChunks" with PK of "ID" of type String

func toItemAndChunks(id int64, x interface{}) itemMap {
	b, err := json.Marshal(&x)
	if err != nil {
		panic(err)
	}

	u, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	cs := getChunks(b, u.String(), id)
	m := itemMap{
		ID:         id,
		UUID:       u.String(),
		Chunks:     cs,
		ChunkCount: len(cs),
		UpdateTime: time.Now(),
	}

	return m
}

func getChunkID(id int64, uuid string, cnt int) string {
	return fmt.Sprintf("%d•%s•%d", id, uuid, cnt)
}

func getChunks(b []byte, cv string, id int64) []itemChunk {
	chunks := []itemChunk{}
	chunkCount := 0
	for i := 0; i < len(b); i += maxItemLen {
		chunkCount++

		end := i + maxItemLen
		if end > len(b) {
			end = len(b)
		}

		c := itemChunk{
			ID:   getChunkID(id, cv, chunkCount),
			Body: b[i:end],
		}

		chunks = append(chunks, c)
	}

	return chunks
}

type itemMap struct {
	ID         int64       // matches ID of resource we're chunking (PK)
	UpdateTime time.Time   // the latest UpdateTime "wins" when writing to itemMap
	ChunkCount int         // number of chunks resource has been broken up into
	Chunks     []itemChunk // the chunks that comprise the item when concatenated
	UUID       string      // prevents case where UpdateTime is same for requests
}

// TODO: bytes or string length?
const maxItemLen = 100 // 400kbps in DynamoDB

type itemChunk struct {
	ID   string // "ID•UUID•ChunkIdx" (PK)
	Body []byte // contents of the resource split among chunks
}

// Get retrieves the item and its concatenated chunks stored in DynamoDB
func Get(id int64, svc *dynamodb.DynamoDB) ([]byte, error) {
	// get ItemMap to get ItemChunk IDs
	getParams := &dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.FormatInt(id, 10)), // N is a *string
			},
		},
		TableName: aws.String("ItemMap"),
	}
	getRes, err := svc.GetItem(getParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get ItemMap response: %v\n", getRes)

	uuid := getRes.Item["UUID"].String()
	chunkCountString := getRes.Item["ChunkCount"].String()
	chunkCount, err := strconv.Atoi(chunkCountString)
	if err != nil {
		panic(err)
	}

	chunkKeys := []map[string]*dynamodb.AttributeValue{}
	for i := 0; i < chunkCount; i++ {
		chunkID := getChunkID(id, uuid, i)
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
func Upsert(id int64, x interface{}, svc *dynamodb.DynamoDB) error {
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
						B: c.Body, // automatically base64 encoded
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
		TableName:           aws.String("ItemMap"),
		ConditionExpression: aws.String("SET #ut = :ut, #cc = :cc, #uuid = :uuid IF #ut < :ut"),
		ExpressionAttributeNames: map[string]*string{
			"#ut":   aws.String("UpdateTime"),
			"#cc":   aws.String("ChunkCount"),
			"#uuid": aws.String("UUID"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ut": &dynamodb.AttributeValue{
				N: aws.String(m.UpdateTime.Format(time.RFC3339)), // default marshal value
			},
			":cc": &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(m.ChunkCount)),
			},
			":uuid": &dynamodb.AttributeValue{
				N: aws.String(m.UUID),
			},
		},
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.FormatInt(m.ID, 10)),
			},
		},
	}

	putRes, err := svc.PutItem(putParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("response from item put: %v\n", putRes)

	return nil
}

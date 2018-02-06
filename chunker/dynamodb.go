package chunker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
)

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
			ID:       getChunkID(id, cv, chunkCount),
			ChunkIdx: chunkCount,
			Body:     b[i:end],
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

// itemChunk.Body is broken into []byte batches based on this val
// this happens before the base64 encoding for *AttributeValue.B
const maxItemLen = 5 // 400kbs in DynamoDB

type itemChunk struct {
	ID       string // "ID•UUID•ChunkIdx" (PK)
	ChunkIdx int    // starts at 1
	Body     []byte // contents of the resource split among chunks
}

// custom logic to sort the responses from ItemChunk
type byChunkNumber []map[string]*dynamodb.AttributeValue

func (c byChunkNumber) Len() int {
	return len(c)
}
func (c byChunkNumber) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c byChunkNumber) Less(i, j int) bool {
	ii, err := strconv.Atoi(*c[i]["ChunkIdx"].N)
	if err != nil {
		panic(err)
	}
	jj, err := strconv.Atoi(*c[j]["ChunkIdx"].N)
	if err != nil {
		panic(err)
	}

	return ii < jj
}

// Get retrieves the item and its concatenated chunks stored in DynamoDB
func Get(id int64, svc *dynamodb.DynamoDB, structPtr interface{}) error {
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

	uuid := *getRes.Item["UUID"].S
	n := *getRes.Item["ChunkCount"].N
	chunkCount, err := strconv.Atoi(n)
	if err != nil {
		panic(err)
	}

	chunkKeys := []map[string]*dynamodb.AttributeValue{}
	for i := 1; i <= chunkCount; i++ {
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
	fmt.Printf("get ItemChunks request: %v\n", getChunkParams)
	getChunkRes, err := svc.BatchGetItem(getChunkParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get ItemChunks response: %v\n", getChunkRes)

	sort.Sort(byChunkNumber(getChunkRes.Responses["ItemChunks"]))

	combined := []byte{}
	w := bytes.NewBuffer(combined)
	for _, c := range getChunkRes.Responses["ItemChunks"] {
		_, err := w.Write(c["Body"].B)
		if err != nil {
			panic(err)
		}
	}

	return json.Unmarshal(w.Bytes(), structPtr)
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
					"ChunkIdx": &dynamodb.AttributeValue{
						N: aws.String(strconv.Itoa(c.ChunkIdx)),
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
	fmt.Printf("batch chunk write request: %v\n", batchParams)

	batchRes, err := svc.BatchWriteItem(batchParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("batch chunk write response: %v\n", batchRes)

	// (2)
	putParams := &dynamodb.PutItemInput{
		TableName:           aws.String("ItemMap"),
		ConditionExpression: aws.String("#ut <= :ut or attribute_not_exists(#ut)"),
		ExpressionAttributeNames: map[string]*string{
			"#ut": aws.String("UpdateTime"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ut": &dynamodb.AttributeValue{
				S: aws.String(m.UpdateTime.Format(time.RFC3339)), // default marshal value
			},
		},
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: aws.String(strconv.FormatInt(m.ID, 10)),
			},
			"ChunkCount": &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(m.ChunkCount)),
			},
			"UUID": &dynamodb.AttributeValue{
				S: aws.String(m.UUID),
			},
			"UpdateTime": &dynamodb.AttributeValue{
				S: aws.String(m.UpdateTime.Format(time.RFC3339)),
			},
		},
	}
	fmt.Printf("itemMap put request: %v\n", putParams)

	putRes, err := svc.PutItem(putParams)
	if err != nil {
		panic(err)
	}
	fmt.Printf("itemMap put response: %v\n", putRes)

	return nil
}

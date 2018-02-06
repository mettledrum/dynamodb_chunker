# Chunker
[best practices for items](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GuidelinesForItems.html) has a slick demo for keeping track of multiple items that could be too long for the item length in DynamoDB.

This is an attempt to implement a client that can automatically chunk up large upsert requests.  It writes multiple versions of the chunks as well as a map item that keeps track of the chunks.

If there are concurrent `Upsert` calls racing one another, only the call made most recently will succeed when trying to update the map item.

## Prerequisites
- table of "ItemMap" with PK of "ID" of type Number
- table of "ItemChunks" with PK of "ID" of type String

## TODO
- get rid of panic(err) junk
- test with a lot of concurrency
- make a script to create table with prerequisites
- interfaces
- add clean up script that kills orphaned ItemChunks that could be run in the background
- configurable max chunk size

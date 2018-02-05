# Chunker
[best practices for items](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GuidelinesForItems.html) has a slick demo for keeping track of multiple items that could be too long for the item length in DynamoDB.

This is an attempt to implement a client that can automatically chunk up large upsert requests.  It writes multiple versions of the chunks as well as a map item that keeps track of the chunks.

If there are concurrent `Upsert` calls racing one another, only the call made most recently will succeed when trying to update the map item.

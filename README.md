# Split and Move

## Intro

This Python script balances out a MongoDB sharded cluster. The script determines availabke shards, splits chunks to manageable size (so the critical area is small), moves the chunks, then remerges the chunks.

## Setup

Five variables are required:

* Global:
  - COLL_MINKEY: minimum possible value of the shard key (replaces MinKey())
  - COLL_MAXKEY: maximum possible value of the shard key (replaces MaxKey())
  - MOVE_CHUNK_SIZE: size to split chunks into in bytes
* Main:
  - conn_string: MongoDB connection string for sharded cluster
  - namespace: namespace of database & colelction to perform the balancing

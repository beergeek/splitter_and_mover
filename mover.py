try:
  import configparser
  import gi
  import datetime
  import json
  import logging
  import math
  import pymongo
  import sys
  from bson.binary import UUID
  from bson import MinKey, MaxKey
  from copy import deepcopy
  from pprint import pprint
  from time import sleep
  from tqdm import tqdm
except ImportError as e:
  print(e)
  exit(1)

# Global Variables
COLL_MINKEY = '10000000000000000000'
COLL_MAXKEY = '99999999999999999999'
MOVE_CHUNK_SIZE = 16777216 #16MB

def mdb_client(db_data, debug=True):
  try:
    client = pymongo.MongoClient(db_data, serverSelectionTimeoutMS=10000, UuidRepresentation="standard")
    client.admin.command('hello')
    return client, None
  except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.ConnectionFailure) as e:
    return None, f"Cannot connect to database, please check settings in config file: {e}"

# Get the UUID of a collection from the namespace
def get_coll_uuid(client, ns):
  try:
    print(ns)
    coll_uuid = client.find_one({"_id": ns},{"_id": 0, "uuid": 1})
    return coll_uuid["uuid"], None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot get collection UUID: {e}"

def stop_balancer(client):
  try:
    client.admin.command('balancerStop')
    count = 0
    while True:
      status = client.admin.command('balancerStatus')
      if status["mode"] == 'off':
        break
      count += 1
      if count == 60:
        return None, "Could not stop balancer"
      sleep(1)
    return True, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot stop the balancer: {e}"

# Get all shards
def get_shards(client):
  try:
    shards = client.find({},{"_id": 1})
    return shards, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot get shards: {e}"

# Get the chunks for a collection
def get_chunks(client, collection_uuid):
  try:
    chunks = client.aggregate(
      [
        {
          "$match": {
           "uuid": collection_uuid
          }
        },
        {
          "$group": {
            "_id": "$shard",
            "chunks": {
              "$push": {
                "min": "$min",
                "max": "$max",
                "_id": "$_id"
              }
            },
            "chunk_count": {
               "$sum": 1
            }
          }
        }
      ]
    )
    return chunks, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot get chunks: {e}"

# Create list from cursor
def shard_cursor_to_list(cursor):
  result_set = []
  for i in cursor:
    result_set.append(i["_id"])
  return result_set

# Get all the shards and number of chunks per shard
def get_total_shards_and_count(all_shards, current_shards, max_chunks_per_shard):
  total_shards = []
  for shard in all_shards:
    found = False
    for current_shard in current_shards:
      if shard == current_shard["_id"]:
        # only include if shard has less chunks than what we want (e.g. it is not even)
        if current_shard["chunk_count"] <= max_chunks_per_shard:
          total_shards.append({"_id": current_shard["_id"], "chunk_count": current_shard["chunk_count"]})
        found = True
        break
    if found == False:
      total_shards.append({"_id": shard, "chunk_count": 0})
  return total_shards

# Move a chunk using a `find` command
def move_chunk(client, recipient, ns, find_point):
  try:
    result = client.admin.command({
        "moveChunk": ns,
        "find": {"location": find_point},
        "to": recipient,
        "_secondaryThrottle": True,
        "_waitForDelete": True,
        "writeConcern": {"w": "majority", "j": True}
      }
    )
    return result, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot move chunks: {e}"

# split a chunk at a location
def split_chunk(client, ns, split_point):
  try:
    result = client.admin.command(
      {
        "split": ns,
        "middle": {"location": split_point}
      }
    )
    return result, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot split chunks: {e}"

# merge chunks for a given boundary
def merge_chunks(client, ns, bounds):
  try:
    result = client.admin.command(
      {
        "mergeChunks": ns,
        "bounds": bounds
      }
    )
    return result, None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot merge chunks: {e}"

# Get the chunk size in bytes
def get_chunk_size(client, ns, key, bounds):
  try:
    result = client.admin.command(
      {
        "datasize": ns,
        "keyPattern": key,
        "min": bounds[0],
        "max": bounds[1]
      }
    )
    return int(math.floor(result["size"])), None
  except pymongo.errors.OperationFailure as e:
    return None, f"Cannot get chunk size: {e}"

# Determine where to split the chunks and return as a lit
def split_middle_points(bounds, required_chunks):
  split_points = []
  if type(bounds[0]["location"]) is MinKey:
    bounds[0]["location"] = COLL_MINKEY
  if type(bounds[1]["location"]) is MaxKey:
    bounds[1]["location"] = COLL_MAXKEY
  print(f"REQUIRED CHUNKS: {required_chunks}")
  factor = math.floor((int(bounds[1]["location"]) - int(bounds[0]["location"])) / required_chunks)
  print(f"FACTOR: {factor}")
  for i in range(required_chunks - 1):
    split_points.append(str(int(bounds[0]["location"]) + (factor * (i + 1))))
  return split_points


def main():
  conn_string = "mongodb+srv://<CONNECTION_STRING>"
  namespace = "testRandom.testData"
  
  client, err = mdb_client(conn_string, debug=True)
  if err != None:
    print(err)
    logging.error(err)
    sys.exit(1)

  # get collection UUID
  collection_uuid, err = get_coll_uuid(client["config"]["collections"], namespace)
  if err != None:
    logging.error(err)
    sys.exit(1)
  print(f"COLLECTION UUID: {collection_uuid}")

  # stop balancer
  _, err = stop_balancer(client)
  if err != None:
    print(err)
    logging.error(err)
    sys.exit(1)

  # get shards/chunks for collection
  current_chunks, err = get_chunks(client["config"]["chunks"], collection_uuid)
  if err != None:
    print(err)
    logging.error(err)
    sys.exit(1)
  total_chunks = 0
  current_to_process = []
  for i in current_chunks:
    current_to_process.append(i)
    total_chunks += i["chunk_count"]

  # get available shards
  shards, err = get_shards(client["config"]["shards"])
  if err != None:
    logging.error(err)
    sys.exit(1)
  available_shard = shard_cursor_to_list(shards)
  print(f"SHARDS AVAILABLE: {available_shard}")

  # calculate chunks per shard
  chunks_per_shard = math.ceil(total_chunks / len(available_shard))
  print(f"CHUNKS PER SHARD: {chunks_per_shard}")

  # determine shard available
  total_shards = get_total_shards_and_count(available_shard, current_to_process, chunks_per_shard)
  print(f"SHARDS IN USE: {total_shards}\n\n")

  # move chunks
  for shard in current_to_process:
    print(f"PROCESSING: {shard['_id']}")
    chunks_to_move = shard["chunk_count"] - chunks_per_shard
    print(f"CHUNKS TO MOVE: {chunks_to_move}")
    if chunks_to_move <= 0:
      print(f"NO CHUNKS TO MOVE")
      continue
    
    # move required chunks
    for i in range(chunks_to_move):
      for idx, av_shard in enumerate(total_shards):
        if av_shard["chunk_count"] < chunks_per_shard:
          # Get bounds of chunk
          bounds = [shard['chunks'][i]["min"],shard['chunks'][i]["max"]]
          print(f"BOUNDS: {bounds}\nCHUNK: {shard['chunks'][i]['_id']}")

          # Determine the chunk size
          chunk_size, err = get_chunk_size(client, namespace, "location", bounds)
          if err != None:
            logging.error(err)
            sys.exit(1)
          print(f"CHUNK SIZE: {chunk_size}")

          # determine how many times we need to split the chunk to get chunks the size of MOVE_CHUNK_SIZE
          split_times = math.ceil(chunk_size / MOVE_CHUNK_SIZE)
          print(f"SPLIT TIMES: {split_times - 1}")

          # Perform the splits if we need to split the chunk
          if split_times > 1:
            split_list = split_middle_points(bounds, split_times)
            print(f"SPLIT LIST: {split_list}")
            for split in split_list:
              print(f"SPLITING: {split} {shard['_id']} to {av_shard['_id']}")
              _, err = split_chunk(client, namespace, split)
              if err != None:
                logging.error(err)
                sys.exit(1)

            # Move split chunks
            for move_point in split_list:
              print(f"MOVING: {move_point} {shard['_id']} to {av_shard['_id']}")
              x, err = move_chunk(client, av_shard["_id"], namespace, move_point)
              print(x)
              if err != None:
                logging.error(err)
                sys.exit(1)

          # move initial chunk
          print(f"MOVING: {shard['chunks'][i]['min']} {shard['_id']} to {av_shard['_id']}")
          x, err = move_chunk(client, av_shard["_id"], shard['chunks'][i]["min"]["location"])
          print(x)
          if err != None:
            logging.error(err)
            sys.exit(1)

          # re-merge chunks
          _, err = merge_chunks(client, namespace, bounds)
          if err != None:
            logging.error(err)
            sys.exit(1)

          # Increment the number of chunks per shard
          total_shards[idx]["chunk_count"] += 1
          break # use this shard then move on
        print("\n")
  print(f"\n\n{total_shards}")

if __name__ == "__main__":
  logger = logging.getLogger(__name__)
  main()
import asyncpg
import pymongo
import asyncio
import time

async def main(table:str, db:str, collection:str, postgres: str, mongo: str):
    """
    table: table name of psql from where data will be copied
    db: db name of mongo
    collection = collection name of mongo
    postgres: connection string of postgres db
    mongo: connection string of mongo db
    """

    print("Working, please wait")
    tm = time.time()
    postgres = await asyncpg.create_pool(postgres)
    mongo = pymongo.MongoClient(mongo)
    
    collection = mongo[db][collection]
    rows = await postgres.fetch(f"SELECT * FROM {table}")
    rows = list(rows)

    for row in rows:
        keys = list(row.keys())
        values = list(row.values())

        document = {}
        i = 0
        for key in keys:
            document[key] = values[i]
            i += 1
    
        collection.insert_one(document=document)
    print(f"Success: finished in {round(time.time()-tm, 1)} sec")

def postgres_to_mongo(table:str, db:str, collection:str, postgres: str, mongo: str):
    asyncio.run(main(table=table, db=db, collection=collection, postgres=postgres, mongo=mongo ))
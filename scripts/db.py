import asyncpg
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

async def init_db(app):
    retries = 5
    for attempt in range(retries):
        try:
            app["db"] = await asyncpg.connect(**DB_CONFIG)

            print(" Connected to PostgreSQL")
            return
        except Exception as e:
            print(f" DB attempt {attempt+1}/{retries} failed: {e}")
            await asyncio.sleep(2)

    raise RuntimeError("Could not connect to the database after several attempts")

async def close_db(app):
    await app["db"].close()
    print("DB connection closed")
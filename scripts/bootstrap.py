import asyncpg
import os
import asyncio
import pandas as pd
import gzip
import shutil
import aiohttp
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

DATA_DIR = "data/raw"
URLS = {
    "movies": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
}

MOVIES_TABLE = "movies"
RATINGS_TABLE = "rating_movies"


async def ensure_tables(conn):
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {MOVIES_TABLE} (
            tconst TEXT PRIMARY KEY,
            title_type TEXT,
            primary_title TEXT,
            original_title TEXT,
            is_adult BOOLEAN,
            start_year INT,
            end_year INT,
            runtime_minutes INT,
            genres TEXT
        )
    """)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RATINGS_TABLE} (
            tconst TEXT PRIMARY KEY,
            average_rating FLOAT,
            num_votes INT
        )
    """)


async def table_has_data(conn, table_name: str) -> bool:
    row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {table_name}")
    return row["count"] > 0


async def download_and_extract(name, url):
    os.makedirs(DATA_DIR, exist_ok=True)
    gz_path = os.path.join(DATA_DIR, f"{name}.tsv.gz")
    tsv_path = os.path.join(DATA_DIR, f"{name}.tsv")

    if not os.path.exists(gz_path):
        print(f"Descargando {name}...")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                with open(gz_path, "wb") as f:
                    f.write(await resp.read())

    if not os.path.exists(tsv_path):
        print(f"Descomprimiendo {gz_path}")
        with gzip.open(gz_path, "rb") as f_in, open(tsv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return tsv_path


def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["titleType"] == "movie"]
    df = df.replace("\\N", None)
    df["isAdult"] = df["isAdult"].astype(bool)
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce").fillna(0).astype(int)
    df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce").fillna(0).astype(int)
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce").fillna(0).astype(int)
    df["primaryTitle"] = df["primaryTitle"].fillna("")
    df["originalTitle"] = df["originalTitle"].fillna("")
    df["genres"] = df["genres"].fillna("")

    return df


def clean_ratings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("\\N", None)
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce").fillna(0).astype(float)
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").fillna(0).astype(int)

    return df


async def load_data(pool):
    async with pool.acquire() as conn:
        await ensure_tables(conn)

        has_movies = await table_has_data(conn, MOVIES_TABLE)
        has_ratings = await table_has_data(conn, RATINGS_TABLE)

        if has_movies and has_ratings:
            print("Las tablas ya tienen datos. Nada que hacer.")
            return

        print("Las tablas están vacías. Cargando datos...")

        movies_path = await download_and_extract("movies", URLS["movies"])
        ratings_path = await download_and_extract("ratings", URLS["ratings"])

        movies_df = pd.read_csv(movies_path, sep="\t", dtype=str)
        ratings_df = pd.read_csv(ratings_path, sep="\t", dtype=str)

        movies_df = clean_movies(movies_df)
        ratings_df = clean_ratings(ratings_df)

        async with conn.transaction():
            await conn.executemany(
                f"""INSERT INTO {MOVIES_TABLE} 
                    (tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT DO NOTHING
                """,
                list(movies_df[[
                    "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
                    "startYear", "endYear", "runtimeMinutes", "genres"
                ]].itertuples(index=False, name=None))
            )

            await conn.executemany(
                f"""INSERT INTO {RATINGS_TABLE} (tconst, average_rating, num_votes)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                """,
                list(ratings_df[["tconst", "averageRating", "numVotes"]].itertuples(index=False, name=None))
            )

        print("✅ Datos importados correctamente.")

async def wait_for_postgres(retries=10, delay=2):
    for attempt in range(retries):
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            await conn.close()
            print("✅ PostgreSQL está disponible")
            return
        except Exception as e:
            print(f"⏳ Intento {attempt + 1}/{retries} fallido: {e}")
            await asyncio.sleep(delay)
    raise RuntimeError("❌ No se pudo conectar a la base de datos tras múltiples intentos.")


async def main():
    await wait_for_postgres()
    pool = await asyncpg.create_pool(**DB_CONFIG)
    try:
        await load_data(pool)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

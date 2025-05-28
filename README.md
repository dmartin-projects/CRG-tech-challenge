# IMDb Movies API

This project is a lightweight asynchronous REST API built with [aiohttp](https://docs.aiohttp.org/) and [asyncpg](https://magicstack.github.io/asyncpg/) that serves IMDb movie data from a PostgreSQL database.

## Features

- Exposes REST endpoints to:
  - List movies with filtering, sorting, and pagination
  - Retrieve movie details by ID (`tconst`)
  - Add new movies via POST
- Automatically downloads IMDb datasets on first run if the database is empty
- Generates IMDb links for each movie from its `tconst`
- Fully Dockerized for local or deployment use

## Datasets Used

On first launch, if the database is empty, the following IMDb datasets are automatically downloaded:

- [`title.basics.tsv.gz`](https://datasets.imdbws.com/title.basics.tsv.gz)
- [`title.ratings.tsv.gz`](https://datasets.imdbws.com/title.ratings.tsv.gz)

These are saved to the `data/raw/` folder and loaded into the database after decompressing and cleaning.

## Running the Project (Docker)

2. **Configure environment variables**

Create a `.env` file in the root directory:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=imdb
POSTGRES_HOST=db
POSTGRES_PORT=5432
```

3. **Start the app with Docker Compose**

```bash
docker-compose up --build
```

This will:

- Start PostgreSQL in a container
- Wait for the database to be ready
- Download and load the IMDb data into the database (if empty)
- Launch the aiohttp server on `http://localhost:8181`

## API Endpoints

### `GET /movies`

List movies with optional filters and sorting:

Query parameters:
- `genre=Drama`
- `rating=7.5`
- `sort_by=year,rating`
- `order=desc,asc`
- `limit=50`
- `offset=0`

### `GET /movies/{id}`

Retrieve a movie by its IMDb `tconst`, e.g.:

```
GET /movies/tt0111161
```

### `POST /movies`

Add a new movie (rating is optional):

```json
{
  "tconst": "tt9999999",
  "title": "Custom Film",
  "genre": "Drama",
  "year": 2025,
  "rating": 8.0,
  "runtime": 120
}
```

## Project Structure

```
.
├── app/
│   ├── routes.py          # aiohttp handlers
│   ├── models.py          # Pydantic schemas
│   └── ...
├── scripts/
│   └── bootstrap.py       # Initializes DB and loads data if needed
├── .env                   # Environment variables
├── Dockerfile
├── docker-compose.yml
└── main.py                # App entrypoint
```

## Notes

- IMDb data is only downloaded if `movies` and `rating_movies` tables are empty.
- All database connections are managed via a connection pool.
- Each movie includes a generated `imdb_link` like:
  ```
  https://www.imdb.com/title/tt0111161/
  ```

---

## Tech Stack

- Python 3.10
- aiohttp
- asyncpg
- pandas
- PostgreSQL
- Docker + Docker Compose

---

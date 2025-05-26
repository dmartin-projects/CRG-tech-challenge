
from app.schemas.api_response_movie import MovieResponse
from app.schemas.api_create_movie import MovieCreateRequest

from aiohttp import web
from asyncpg import Connection, UniqueViolationError

async def create_movie(request: web.Request) -> web.Response:
    pool = request.app["db"]
    try:
        body = await request.json()
        data = MovieCreateRequest(**body)
    except Exception as e:
        return web.json_response({"error": "Invalid input", "details": str(e)}, status=400)

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO movies (tconst, primary_title, genres, start_year, runtime_minutes)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    """,
                    data.tconst, data.title, data.genre, data.year, data.runtime
                )
                await conn.execute(
                    """
                    INSERT INTO rating_movies (tconst, average_rating, num_votes)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                    """,
                    data.tconst, data.rating, data.votes
                )
    except UniqueViolationError:
        return web.json_response({"error": "Movie with this tconst already exists"}, status=409)
    except Exception as e:
        return web.json_response({"error": "Database error", "details": str(e)}, status=500)

    movie = MovieResponse(
        tconst=data.tconst,
        title=data.title,
        genre=data.genre,
        year=data.year,
        rating=data.rating,
        runtime=data.runtime
    )

    return web.json_response(movie.dict(), status=201)


async def get_movie_by_id(request: web.Request) -> web.Response:
    conn: Connection = request.app["db"]
    tconst = request.match_info.get("id")

    if not tconst:
        return web.json_response({"error": "Movie ID is required"}, status=400)

    sql = """
        SELECT
            m.tconst,
            m.primary_title,
            m.genres,
            m.start_year,
            r.average_rating,
            m.runtime_minutes
        FROM movies m
        JOIN rating_movies r ON m.tconst = r.tconst
        WHERE m.tconst = $1
    """

    row = await conn.fetchrow(sql, tconst)

    if not row:
        return web.json_response({"error": "Movie not found"}, status=404)

    movie = MovieResponse(
        tconst=row["tconst"],
        title=row["primary_title"],
        genre=row["genres"],
        year=int(row["start_year"]),
        rating=float(row["average_rating"]),
        runtime=int(row["runtime_minutes"]),
    )

    return web.json_response(movie.dict())

async def get_movies(request: web.Request) -> web.Response:
    conn: Connection = request.app["db"]

    genre = request.query.get("genre")
    min_rating = request.query.get("rating")
    sort_by_fields = request.query.get("sort_by", "title").split(",")
    order_fields = request.query.get("order", "asc").split(",")

    limit = int(request.query.get("limit", 50))
    offset = int(request.query.get("offset", 0))

    allowed_sorts = {"title", "year", "rating"}
    allowed_order = {"asc", "desc"}

    if not all(field in allowed_sorts for field in sort_by_fields):
        return web.json_response({"error": "Invalid sort_by field"}, status=400)

    while len(order_fields) < len(sort_by_fields):
        order_fields.append("asc")  

    if not all(direction in allowed_order for direction in order_fields):
        return web.json_response({"error": "Invalid order value"}, status=400)

    if limit < 1 or limit > 200:
        return web.json_response({"error": "Limit must be between 1 and 200"}, status=400)

    sql = """
        SELECT
            m.tconst,
            m.primary_title,
            m.genres,
            m.start_year,
            r.average_rating,
            m.runtime_minutes
        FROM movies m
        JOIN rating_movies r ON m.tconst = r.tconst
        WHERE 1=1
    """
    params = []
    param_index = 1

    if genre:
        sql += f" AND m.genres ILIKE ${param_index}"
        params.append(f"%{genre}%")
        param_index += 1

    if min_rating:
        sql += f" AND r.average_rating = ${param_index}"
        params.append(float(min_rating))
        param_index += 1

    column_map = {
        "title": "m.primary_title",
        "year": "m.start_year",
        "rating": "r.average_rating",
    }

    order_clauses = []
    for field, direction in zip(sort_by_fields, order_fields):
        order_clauses.append(f"{column_map[field]} {direction.upper()}")

    sql += f" ORDER BY {', '.join(order_clauses)}"

    sql += f" LIMIT ${param_index} OFFSET ${param_index + 1}"
    params.extend([limit, offset])

    rows = await conn.fetch(sql, *params)

    result = [
        MovieResponse(
            tconst=row["tconst"],
            title=row["primary_title"],
            genre=row["genres"],
            year=int(row["start_year"]),
            rating=float(row["average_rating"]),
            runtime=int(row["runtime_minutes"]),
        ).dict()
        for row in rows
    ]

    return web.json_response({
        "limit": limit,
        "offset": offset,
        "results": result
    })


def setup_routes(app):
    app.router.add_get("/movies", get_movies)
    app.router.add_get("/movies/{id}", get_movie_by_id)
    app.router.add_post("/movies", create_movie)


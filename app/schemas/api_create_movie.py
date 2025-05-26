from pydantic import BaseModel, constr, conint, confloat

class MovieCreateRequest(BaseModel):
    tconst: constr(strip_whitespace=True)
    title: str
    genre: str
    year: conint(ge=1800, le=2100)
    rating: confloat(ge=0.0, le=10.0) = 0.0
    votes: int = 0
    runtime: conint(ge=1)

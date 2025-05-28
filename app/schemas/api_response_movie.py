from pydantic import BaseModel, HttpUrl,computed_field
from typing import Optional

class MovieResponse(BaseModel):
    tconst:str
    title: str
    genre: str
    year: int
    rating: float
    runtime: int

    @computed_field
    @property
    def imdb_link(self) -> str:
        return f"https://www.imdb.com/title/{self.tconst}/"
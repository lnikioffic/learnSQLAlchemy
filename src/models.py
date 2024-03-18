from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import String
from typing import Annotated



str_200 = Annotated[str, 200]

class Base(DeclarativeBase):
    type_annotation_map = {
        str_200: String(200)
    }

    def __repr__(self) -> str:
        cols = []
        for col in self.__table__.columns.keys():
            cols.append(f'{col}={getattr(self, col)}')
        return f'<{self.__class__.__name__} {','.join(cols)}>'
    
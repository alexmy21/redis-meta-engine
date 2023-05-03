from pydantic import BaseModel
from typing import Optional

class Schema(BaseModel):
    name: str
    description: str
    kind: str
    tax: Optional[float] = None

    class Config:
        orm_mode = True

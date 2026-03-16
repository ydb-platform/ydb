from datetime import timedelta
from typing import Any, Dict, Optional, Type

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import BaseModel, Field

from beanie.odm.utils.pydantic import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from pydantic import ConfigDict


class ItemSettings(BaseModel):
    name: Optional[str] = None

    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Dict[Any, Any] = Field(default_factory=dict)
    projection: Optional[Dict[str, Any]] = None

    motor_db: Optional[AsyncIOMotorDatabase] = None
    motor_collection: Optional[AsyncIOMotorCollection] = None

    union_doc: Optional[Type] = None
    union_doc_alias: Optional[str] = None
    class_id: str = "_class_id"

    is_root: bool = False

    if IS_PYDANTIC_V2:
        model_config = ConfigDict(
            arbitrary_types_allowed=True,
        )
    else:

        class Config:
            arbitrary_types_allowed = True

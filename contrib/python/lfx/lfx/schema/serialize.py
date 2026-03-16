from uuid import UUID

# Note: Previously UUIDstr was Annotated[UUID, BeforeValidator(str_to_uuid)]
# but this caused issues with SQLModel/Pydantic v2 where the FieldInfo subclass
# attributes (like primary_key) were lost when processing Annotated types with metadata.
# Using plain UUID fixes the SQLAlchemy "could not assemble any primary key columns" error.
# We leave str_to_uuid to import's compatibility.
str_to_uuid = None
UUIDstr = UUID

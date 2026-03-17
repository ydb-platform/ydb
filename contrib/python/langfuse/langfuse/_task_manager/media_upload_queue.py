from typing import Optional, TypedDict


class UploadMediaJob(TypedDict):
    media_id: str
    content_type: str
    content_length: int
    content_bytes: bytes
    content_sha256_hash: str
    trace_id: str
    observation_id: Optional[str]
    field: str

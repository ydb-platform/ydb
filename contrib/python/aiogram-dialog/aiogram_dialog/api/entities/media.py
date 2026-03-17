from dataclasses import dataclass
from pathlib import Path

from aiogram.types import ContentType


@dataclass
class MediaId:
    file_id: str
    file_unique_id: str | None = None

    def __eq__(self, other):
        if type(other) is not MediaId:
            return False
        if self.file_unique_id is None or other.file_unique_id is None:
            return self.file_id == other.file_id
        return self.file_unique_id == other.file_unique_id


class MediaAttachment:
    def __init__(
            self,
            type: ContentType,
            url: str | None = None,
            path: str | Path | None = None,
            file_id: MediaId | None = None,
            use_pipe: bool = False,
            **kwargs,
    ):
        if not (url or path or file_id):
            raise ValueError("Neither url nor path not file_id are provided")
        self.type = type
        self.url = url
        self.path = path
        self.file_id = file_id
        self.use_pipe = use_pipe
        self.kwargs = kwargs

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        return (
            self.type == other.type and
            self.url == other.url and
            self.path == other.path and
            self.file_id == other.file_id and
            self.use_pipe == other.use_pipe and
            self.kwargs == other.kwargs
        )

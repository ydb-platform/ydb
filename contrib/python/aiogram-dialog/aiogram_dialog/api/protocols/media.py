from abc import abstractmethod
from typing import Protocol

from aiogram.types import ContentType

from aiogram_dialog.api.entities import MediaId


class MediaIdStorageProtocol(Protocol):
    @abstractmethod
    async def get_media_id(
            self,
            path: str | None,
            url: str | None,
            type: ContentType,
    ) -> MediaId | None:
        raise NotImplementedError

    @abstractmethod
    async def save_media_id(
            self,
            path: str | None,
            url: str | None,
            type: ContentType,
            media_id: MediaId,
    ) -> None:
        raise NotImplementedError

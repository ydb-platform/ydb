from pathlib import Path

from aiogram.types import ContentType

from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.api.internal import TextWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import WhenCondition
from aiogram_dialog.widgets.text import Const
from .base import Media


class StaticMedia(Media):
    def __init__(
            self,
            *,
            path: TextWidget | str | Path | None = None,
            url: TextWidget | str | None = None,
            type: ContentType = ContentType.PHOTO,
            use_pipe: bool = False,
            media_params: dict | None = None,
            when: WhenCondition = None,
    ):
        super().__init__(when=when)
        if not (url or path):
            raise ValueError("Neither url nor path are provided")
        self.type = type
        if isinstance(path, Path):
            path = Const(str(path))
        elif isinstance(path, str):
            path = Const(path)
        self.path = path
        if isinstance(url, str):
            url = Const(url)
        self.url = url
        self.use_pipe = use_pipe
        self.media_params = media_params or {}

    async def _render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        if self.url:
            url = await self.url.render_text(data, manager)
        else:
            url = None
        if self.path:
            path = await self.path.render_text(data, manager)
        else:
            path = None

        return MediaAttachment(
            type=self.type,
            url=url,
            path=path,
            use_pipe=self.use_pipe,
            **self.media_params,
        )

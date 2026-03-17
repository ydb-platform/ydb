"""
Dynamic media.

Originally developed in aiogram-dialog-extras project
https://github.com/SamWarden/aiogram_dialog_extras
"""

from collections.abc import Callable
from operator import itemgetter

from aiogram_dialog import DialogManager
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.widgets.common import WhenCondition
from aiogram_dialog.widgets.media import Media

MediaSelector = Callable[[dict], MediaAttachment]


class DynamicMedia(Media):
    def __init__(
            self,
            selector: str | MediaSelector,
            when: WhenCondition = None,
    ):
        super().__init__(when=when)
        if isinstance(selector, str):
            self.selector: MediaSelector = itemgetter(selector)
        else:
            self.selector = selector

    async def _render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        media: MediaAttachment | None = self.selector(data)
        return media

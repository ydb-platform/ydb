
from aiogram.types import LinkPreviewOptions

from aiogram_dialog import DialogManager
from aiogram_dialog.api.internal import LinkPreviewWidget, TextWidget
from aiogram_dialog.widgets.common import BaseWidget, Whenable, WhenCondition


class LinkPreviewBase(Whenable, BaseWidget, LinkPreviewWidget):
    def __init__(self, when: WhenCondition = None):
        super().__init__(when=when)

    async def render_link_preview(
            self, data: dict, manager: DialogManager,
    ) -> LinkPreviewOptions | None:
        if not self.is_(data, manager):
            return None
        return await self._render_link_preview(data, manager)

    async def _render_link_preview(
            self, data: dict, manager: DialogManager,
    ) -> LinkPreviewOptions | None:
        return None


class LinkPreview(LinkPreviewBase):
    def __init__(
            self,
            url: TextWidget | None = None,
            is_disabled: bool = False,
            prefer_small_media: bool = False,
            prefer_large_media: bool = False,
            show_above_text: bool = False,
            when: WhenCondition = None,
    ):
        super().__init__(when=when)
        self.url = url
        self.is_disabled = is_disabled
        self.prefer_small_media = prefer_small_media
        self.prefer_large_media = prefer_large_media
        self.show_above_text = show_above_text

    async def render_link_preview(
            self, data: dict, manager: DialogManager,
    ) -> LinkPreviewOptions | None:
        if not self.is_(data, manager):
            return None
        return await self._render_link_preview(data, manager)

    async def _render_link_preview(
            self, data: dict, manager: DialogManager,
    ) -> LinkPreviewOptions | None:
        return LinkPreviewOptions(
            url=(
                await self.url.render_text(data, manager)
                if self.url
                else None
            ),
            is_disabled=self.is_disabled,
            prefer_small_media=self.prefer_small_media,
            prefer_large_media=self.prefer_large_media,
            show_above_text=self.show_above_text,
        )

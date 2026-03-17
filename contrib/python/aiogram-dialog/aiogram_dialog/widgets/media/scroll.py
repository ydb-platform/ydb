
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import (
    BaseScroll,
    OnPageChangedVariants,
    WhenCondition,
)
from aiogram_dialog.widgets.common.items import (
    ItemsGetterVariant,
    get_items_getter,
)
from .base import Media


class MediaScroll(Media, BaseScroll):
    def __init__(
            self,
            media: Media,
            items: ItemsGetterVariant,
            id: str,
            when: WhenCondition = None,
            on_page_changed: OnPageChangedVariants = None,
    ):
        Media.__init__(self, when=when)
        BaseScroll.__init__(self, id=id, on_page_changed=on_page_changed)
        self.media = media
        self.items_getter = get_items_getter(items)

    async def _render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        items = self.items_getter(data)
        pages = len(items)
        current_page = min(await self.get_page(manager), pages)

        item = items[current_page]
        return await self.media.render_media(
            {
                "current_page": current_page,
                "current_page1": current_page + 1,
                "pages": pages,
                "data": data,
                "item": item,
                "pos": current_page + 1,
                "pos0": current_page,
            },
            manager,
        )

    async def get_page_count(self, data: dict, manager: DialogManager) -> int:
        items = self.items_getter(data)
        return len(items)

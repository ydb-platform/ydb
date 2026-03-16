
from aiogram_dialog.api.entities import MediaAttachment
from aiogram_dialog.api.internal import MediaWidget
from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.utils import add_exception_note
from aiogram_dialog.widgets.common import BaseWidget, Whenable, WhenCondition


class Media(Whenable, BaseWidget, MediaWidget):
    def __init__(self, when: WhenCondition = None):
        super().__init__(when=when)

    @add_exception_note
    async def render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        if not self.is_(data, manager):
            return None
        return await self._render_media(data, manager)

    async def _render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        return None

    def __or__(self, other: "Media") -> "Or":
        # reduce nesting
        if isinstance(other, Or):
            return NotImplemented
        return Or(self, other)

    def __ror__(self, other: "Media") -> "Or":
        # reduce nesting
        return Or(other, self)


class Or(Media):
    def __init__(self, *widgets: Media):
        super().__init__()
        self.widgets = widgets

    async def _render_media(
            self, data: dict, manager: DialogManager,
    ) -> MediaAttachment | None:
        for widget in self.widgets:
            res = await widget.render_media(data, manager)
            if res:
                return res
        return None

    def __ior__(self, other: Media) -> "Or":
        self.widgets += (other,)
        return self

    def __or__(self, other: Media) -> "Or":
        # reduce nesting
        return Or(*self.widgets, other)

    def __ror__(self, other: Media) -> "Or":
        # reduce nesting
        return Or(other, *self.widgets)

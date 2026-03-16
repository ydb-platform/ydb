from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import WhenCondition
from .base import Text


class Progress(Text):
    def __init__(
            self,
            field: str,
            width: int = 10,
            filled="🟥",
            empty="⬜",
            when: WhenCondition = None,
    ):
        super().__init__(when)
        self.field = field
        self.width = width
        self.filled = filled
        self.empty = empty

    async def _render_text(
            self, data: dict, manager: DialogManager,
    ) -> str:
        if manager.is_preview():
            percent = 15
        else:
            percent = data.get(self.field)
        done = round((self.width * percent) / 100)
        rest = self.width - done

        return self.filled * done + self.empty * rest + f" {percent: 2.0f}%"

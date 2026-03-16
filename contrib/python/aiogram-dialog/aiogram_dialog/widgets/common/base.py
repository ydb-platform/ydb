from typing import Any, Optional

from aiogram_dialog import DialogManager
from aiogram_dialog.api.internal import Widget


class BaseWidget(Widget):
    def managed(self, manager: DialogManager) -> Any:
        return self

    def find(self, widget_id: str) -> Optional["Widget"]:
        return None

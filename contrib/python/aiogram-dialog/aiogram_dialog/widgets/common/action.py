import re
from typing import Any

from aiogram_dialog.api.exceptions import InvalidWidgetIdError
from aiogram_dialog.api.protocols import DialogManager
from .base import BaseWidget

ID_PATTERN = re.compile("^[a-zA-Z0-9_.]+$")


class Actionable(BaseWidget):
    def __init__(self, id: str | None = None):
        if id and not ID_PATTERN.match(id):
            raise InvalidWidgetIdError(f"Invalid widget id: {id}")
        self.widget_id = id

    def find(self, widget_id: str):
        """Find nested widget or current one by id."""
        if self.widget_id is not None and self.widget_id == widget_id:
            return self
        return None

    def get_widget_data(
            self,
            manager: DialogManager,
            default: Any,
    ) -> Any:
        """Get data for current widget id, setting default if needed."""
        return manager.current_context().widget_data.setdefault(
            self.widget_id,
            default,
        )

    def set_widget_data(
            self,
            manager: DialogManager,
            value: Any,
    ) -> Any:
        """Set data for current widget id."""
        manager.current_context().widget_data[self.widget_id] = value

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.widget_id}>"

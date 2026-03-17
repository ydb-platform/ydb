import typing

from . import WidgetUtils


class DefaultValueOnlyWidgetUtils(WidgetUtils):

    def __init__(self) -> None:
        self._widgets: typing.Dict[str, str] = {}

    def text(self, name: str, defaultValue: str, label: typing.Optional[str] = None):
        self._widgets[name] = defaultValue

    def dropdown(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._widgets[name] = defaultValue

    def combobox(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._widgets[name] = defaultValue

    def multiselect(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._widgets[name] = defaultValue

    def _get(self, name: str) -> str:
        return self._widgets[name]

    def _remove(self, name: str):
        del self._widgets[name]

    def _remove_all(self):
        self._widgets = {}

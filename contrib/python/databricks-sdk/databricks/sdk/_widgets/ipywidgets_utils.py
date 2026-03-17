import typing

from IPython.core.display_functions import display
from ipywidgets.widgets import (ValueWidget, Widget, widget_box,
                                widget_selection, widget_string)

from .default_widgets_utils import WidgetUtils


class DbUtilsWidget:

    def __init__(self, label: str, value_widget: ValueWidget) -> None:
        self.label_widget = widget_string.Label(label)
        self.value_widget = value_widget
        self.box = widget_box.Box([self.label_widget, self.value_widget])

    def display(self):
        display(self.box)

    def close(self):
        self.label_widget.close()
        self.value_widget.close()
        self.box.close()

    @property
    def value(self):
        value = self.value_widget.value
        if type(value) == str or value is None:
            return value
        if type(value) == list or type(value) == tuple:
            return ",".join(value)

        raise ValueError(f"The returned value has invalid type ({type(value)}).")


class IPyWidgetUtil(WidgetUtils):

    def __init__(self) -> None:
        self._widgets: typing.Dict[str, DbUtilsWidget] = {}

    def _register(
        self,
        name: str,
        widget: ValueWidget,
        label: typing.Optional[str] = None,
    ):
        label = label if label is not None else name
        w = DbUtilsWidget(label, widget)

        if name in self._widgets:
            self.remove(name)

        self._widgets[name] = w
        w.display()

    def text(self, name: str, defaultValue: str, label: typing.Optional[str] = None):
        self._register(name, widget_string.Text(defaultValue), label)

    def dropdown(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._register(
            name,
            widget_selection.Dropdown(value=defaultValue, options=choices),
            label,
        )

    def combobox(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._register(
            name,
            widget_string.Combobox(value=defaultValue, options=choices),
            label,
        )

    def multiselect(
        self,
        name: str,
        defaultValue: str,
        choices: typing.List[str],
        label: typing.Optional[str] = None,
    ):
        self._register(
            name,
            widget_selection.SelectMultiple(
                value=(defaultValue,),
                options=[("__EMPTY__", ""), *list(zip(choices, choices))],
            ),
            label,
        )

    def _get(self, name: str) -> str:
        return self._widgets[name].value

    def _remove(self, name: str):
        self._widgets[name].close()
        del self._widgets[name]

    def _remove_all(self):
        Widget.close_all()
        self._widgets = {}

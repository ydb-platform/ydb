from collections.abc import Callable, Sequence

from aiogram_dialog.api.exceptions import InvalidWidgetType
from aiogram_dialog.api.internal import (
    DataGetter,
    LinkPreviewWidget,
    TextWidget,
)
from .data.data_context import CompositeGetter, StaticGetter
from .input import BaseInput, CombinedInput, MessageHandlerFunc, MessageInput
from .kbd import Group, Keyboard
from .link_preview import LinkPreviewBase
from .media import Media
from .text import Format, Multi, Text
from .widget_event import WidgetEventProcessor

WidgetSrc = (
    str |
    TextWidget |
    Keyboard |
    MessageHandlerFunc |
    Media |
    BaseInput |
    LinkPreviewBase
)

SingleGetterBase = DataGetter | dict
GetterVariant = (
    SingleGetterBase |
    list[SingleGetterBase] |
    tuple[SingleGetterBase, ...] |
    None
)


def ensure_text(widget: str | TextWidget | Sequence[TextWidget]) -> TextWidget:
    if isinstance(widget, str):
        return Format(widget)
    if isinstance(widget, Sequence):
        if len(widget) == 1:
            return widget[0]
        return Multi(*widget)
    return widget


def ensure_keyboard(widget: Keyboard | Sequence[Keyboard]) -> Keyboard:
    if isinstance(widget, Sequence):
        if len(widget) == 1:
            return widget[0]
        return Group(*widget)
    return widget


def ensure_input(
    widget: (
            MessageHandlerFunc
            | WidgetEventProcessor
            | BaseInput
            | Sequence[BaseInput]
    ),
) -> BaseInput | None:
    if isinstance(widget, BaseInput):
        return widget
    elif isinstance(widget, Sequence):
        if len(widget) == 0:
            return None
        elif len(widget) == 1:
            return widget[0]
        else:
            return CombinedInput(*widget)
    else:
        return MessageInput(widget)


def ensure_media(widget: Media | Sequence[Media]) -> Media:
    if isinstance(widget, Media):
        return widget
    if len(widget) > 1:  # TODO case selection of media
        raise ValueError("Only one media widget is supported")
    if len(widget) == 1:
        return widget[0]
    return Media()


def ensure_link_preview(
        widget: LinkPreviewWidget | Sequence[LinkPreviewWidget],
) -> LinkPreviewWidget | None:
    if isinstance(widget, LinkPreviewWidget):
        return widget
    if len(widget) > 1:
        raise ValueError("Only one link preview widget is supported")
    if len(widget) == 1:
        return widget[0]
    return None


def ensure_widgets(
        widgets: Sequence[WidgetSrc],
) -> tuple[
    TextWidget,
    Keyboard,
    BaseInput | None,
    Media,
    LinkPreviewWidget | None,
]:
    texts = []
    keyboards = []
    inputs = []
    media = []
    link_preview = []

    for w in widgets:
        if isinstance(w, (str, Text)):
            texts.append(ensure_text(w))
        elif isinstance(w, Keyboard):
            keyboards.append(ensure_keyboard(w))
        elif isinstance(w, (BaseInput, Callable)):
            inputs.append(ensure_input(w))
        elif isinstance(w, Media):
            media.append(ensure_media(w))
        elif isinstance(w, LinkPreviewBase):
            link_preview.append(ensure_link_preview(w))
        else:
            raise InvalidWidgetType(
                f"Cannot add widget of type {type(w)}. "
                f"Only str, Text, Keyboard, BaseInput "
                f"and Callable are supported",
            )
    return (
        ensure_text(texts),
        ensure_keyboard(keyboards),
        ensure_input(inputs),
        ensure_media(media),
        ensure_link_preview(link_preview),
    )


def ensure_data_getter(getter: GetterVariant) -> DataGetter:
    if isinstance(getter, Callable):
        return getter
    elif isinstance(getter, dict):
        return StaticGetter(getter)
    elif isinstance(getter, (list, tuple)):
        return CompositeGetter(*map(ensure_data_getter, getter))
    elif getter is None:
        return StaticGetter({})
    else:
        raise InvalidWidgetType(
            f"Cannot add data getter of type {type(getter)}. "
            f"Only Dict, Callable or List of Callables are supported",
        )

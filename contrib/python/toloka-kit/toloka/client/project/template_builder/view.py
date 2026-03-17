__all__ = [
    'BaseViewV1',
    'ActionButtonViewV1',
    'AlertViewV1',
    'AudioViewV1',
    'CollapseViewV1',
    'DeviceFrameViewV1',
    'DividerViewV1',
    'GroupViewV1',
    'IframeViewV1',
    'ImageViewV1',
    'LabeledListViewV1',
    'LinkViewV1',
    'LinkGroupViewV1',
    'ListViewV1',
    'MapViewV1',
    'MarkdownViewV1',
    'TextViewV1',
    'VideoViewV1'
]
from enum import unique
from typing import List, Any

from .base import (
    BaseTemplate,
    BaseComponent,
    ListDirection,
    ListSize,
    ComponentType,
    VersionedBaseComponentMetaclass,
    base_component_or
)
from ....util._codegen import attribute
from ....util._extendable_enum import ExtendableStrEnum
from ....util._docstrings import inherit_docstrings


class BaseViewV1Metaclass(VersionedBaseComponentMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):

        if 'hint' not in namespace:
            namespace['hint'] = attribute(kw_only=True)
            namespace.setdefault('__annotations__', {})['hint'] = base_component_or(Any)
        if 'label' not in namespace:
            namespace['label'] = attribute(kw_only=True)
            namespace.setdefault('__annotations__', {})['label'] = base_component_or(Any)
        if 'validation' not in namespace:
            namespace['validation'] = attribute(kw_only=True)
            namespace.setdefault('__annotations__', {})['validation'] = BaseComponent
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BaseViewV1(BaseComponent, metaclass=BaseViewV1Metaclass):
    """A base class for components that present data, such as a text, list, audio player, or image.

    Attributes:
        label: A label above the component.
        hint: A hint text.
        validation: Validation rules.
    """

    pass


@inherit_docstrings
class ActionButtonViewV1(BaseViewV1, spec_value=ComponentType.VIEW_ACTION_BUTTON):
    """A button that calls an action.

    For more information, see [view.action-button](https://toloka.ai/docs/template-builder/reference/view.action-button).

    Attributes:
        action: The action.
        label: A label on the button.
    """

    action: BaseComponent


@inherit_docstrings
class AlertViewV1(BaseViewV1, spec_value=ComponentType.VIEW_ALERT):
    """A view used to highlight important information.

    For more information, see [view.alert](https://toloka.ai/docs/template-builder/reference/view.alert).

    Attributes:
        content: The content.
        theme: The theme that sets the background color. The default color is blue.
    """

    @unique
    class Theme(ExtendableStrEnum):
        """A view background color.

        Attributes:
            INFO: Blue.
            SUCCESS: Green.
            WARNING: Yellow.
            DANGER: Red.
        """

        DANGER = 'danger'
        INFO = 'info'
        SUCCESS = 'success'
        WARNING = 'warning'

    content: BaseComponent
    theme: base_component_or(Theme) = attribute(kw_only=True)


@inherit_docstrings
class AudioViewV1(BaseViewV1, spec_value=ComponentType.VIEW_AUDIO):
    """An audio player.

    For more information, see [view.audio](https://toloka.ai/docs/template-builder/reference/view.audio).

    Attributes:
        url: A link to the audio.
        loop:
            * `True` — Play audio in a loop.
            * `False` — Play once.
    """

    url: base_component_or(Any)
    loop: base_component_or(bool) = attribute(kw_only=True)


@inherit_docstrings
class CollapseViewV1(BaseViewV1, spec_value=ComponentType.VIEW_COLLAPSE):
    """An expandable block.

    For more information, see [view.collapse](https://toloka.ai/docs/template-builder/reference/view.collapse).

    Attributes:
        label: The block heading.
        content: The block content.
        default_opened: Initial state of the block:
            `True` — Expanded
            `False` — Collapsed

            Default value: `False`.
    """

    label: base_component_or(Any)
    content: BaseComponent
    default_opened: base_component_or(bool) = attribute(origin='defaultOpened', kw_only=True)


@inherit_docstrings
class DeviceFrameViewV1(BaseViewV1, spec_value=ComponentType.VIEW_DEVICE_FRAME):
    """A view with a frame that is similar to a mobile phone frame.

    For more information, see [view.device-frame](https://toloka.ai/docs/template-builder/reference/view.device-frame).

    Attributes:
        content: The content of the frame.
        full_height: If `True`, the component takes up all the vertical free space.
            Note, that the minimum height required by the component is 400 pixels.
        max_width: The maximum width of the component in pixels.
        min_width: The minimum width of the component in pixels. It takes priority over the `max_width`.
        ratio: A list with the aspect ratio of the component. Specify the relative width first and then the relative height.
            This setting is not used if `full_height=True`.
    """

    content: BaseComponent
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    max_width: base_component_or(float) = attribute(origin='maxWidth', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821


@inherit_docstrings
class DividerViewV1(BaseViewV1, spec_value=ComponentType.VIEW_DIVIDER):
    """A horizontal delimiter.

    For more information, see [view.divider](https://toloka.ai/docs/template-builder/reference/view.divider).

    Attributes:
        label: A label in the center of the delimiter. Note, that line breaks are not supported.
    """

    pass


@inherit_docstrings
class GroupViewV1(BaseViewV1, spec_value=ComponentType.VIEW_GROUP):
    """A view with a frame.

    For more information, see [view.group](https://toloka.ai/docs/template-builder/reference/view.group).

    Attributes:
        content: A content.
        label: A header.
        hint: A hint. To insert a line break in the hint, use `\n`.
    """

    content: BaseComponent


@inherit_docstrings
class IframeViewV1(BaseViewV1, spec_value=ComponentType.VIEW_IFRAME):
    """A frame displaying a web page.

    For more information, see [view.iframe](https://toloka.ai/docs/template-builder/reference/view.iframe).

    Attributes:
        url: The URL of the web page.
        full_height: If `True`, the component takes up all the vertical free space.
            Note, that the minimum height required by the component is 400 pixels.
        max_width: The maximum width of the component in pixels.
        min_width: The minimum width of the component in pixels. It takes priority over the `max_width`.
        ratio: A list with the aspect ratio of the component. Specify the relative width first and then the relative height.
            This setting is not used if `full_height=True`.
    """

    url: base_component_or(str)
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    max_width: base_component_or(float) = attribute(origin='maxWidth', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821


@inherit_docstrings
class ImageViewV1(BaseViewV1, spec_value=ComponentType.VIEW_IMAGE):
    """A component for displaying an image.

    For more information, see [view.image](https://toloka.ai/docs/template-builder/reference/view.image).

    Attributes:
        url: The URL of the image.
        full_height: If `True`, the component takes up all the vertical free space.
            Note, that the minimum height required by the component is 400 pixels.
        max_width: The maximum width of the component in pixels.
        min_width: The minimum width of the component in pixels. It takes priority over the `max_width`.
        no_border: Displaying borders around the image.
            * `True` — The borders are hidden.
            * `False` — The borders are visible.

            Default value: `True`.
        no_lazy_load: Loading mode:
            * `False` — The component starts loading the image when the component becomes visible to a Toloker.
            * `True` — The image is loaded immediately. This mode is useful for icons.

            Default value: `False` — lazy loading is enabled.
        popup: If `True`, a Toloker can open a full sized image in a popup. It is a default behavior.
        ratio: A list with the aspect ratio of the component. Specify the relative width first and then the relative height.
            This setting is not used if `full_height=True`.
        rotatable: If `True`, an image can be rotated.
        scrollable: The way of displaying an image which is larger than the component:
            * `True` — Scroll bars are shown.
            * `False` — The image is scaled to fit the component.

            Note, that images in SVG format with no size specified always fit the component.
    """

    url: base_component_or(Any)
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    max_width: base_component_or(float) = attribute(origin='maxWidth', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    no_border: base_component_or(bool) = attribute(origin='noBorder', kw_only=True)
    no_lazy_load: base_component_or(bool) = attribute(origin='noLazyLoad', kw_only=True)
    popup: base_component_or(bool) = attribute(kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821
    rotatable: base_component_or(bool) = attribute(kw_only=True)
    scrollable: base_component_or(bool) = attribute(kw_only=True)


@inherit_docstrings
class LabeledListViewV1(BaseViewV1, spec_value=ComponentType.VIEW_LABELED_LIST):
    """A list of components with labels placed on the left.

    For more information, see [view.labeled-list](https://toloka.ai/docs/template-builder/reference/view.labeled-list).

    Attributes:
        items: List items.
        min_width: The minimum width of the component.
            If the width is less than the specified value, the list is displayed in compact mode.
            Labels are placed above the items in this mode.
    """

    class Item(BaseTemplate):
        """A labeled list item.

        Attributes:
            content: The content of the item.
            label: An item label.
            center_label: Label vertical alignment.
                * `True` — The label is centered.
                * `False` — The label is aligned to the top of the item content.

                Default value: `False`.
            hint: A hint.
        """

        content: BaseComponent
        label: base_component_or(Any)
        center_label: base_component_or(bool) = attribute(origin='centerLabel', kw_only=True)
        hint: base_component_or(Any) = attribute(kw_only=True)

    items: base_component_or(List[base_component_or(Item)], 'ListBaseComponentOrItem')  # noqa: F821
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)


@inherit_docstrings
class LinkViewV1(BaseViewV1, spec_value=ComponentType.VIEW_LINK):
    """A component showing a link.

    For more information, see [view.link](https://toloka.ai/docs/template-builder/reference/view.link).

    Attributes:
        url: A URL.
        content: A link text.
    """

    url: base_component_or(Any)
    content: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class LinkGroupViewV1(BaseViewV1, spec_value=ComponentType.VIEW_LINK_GROUP):
    """A group of links.

    For more information, see [view.link-group](https://toloka.ai/docs/template-builder/reference/view.link-group).

    Attributes:
        links: A list of links.

    Example:
        >>> import toloka.client.project.template_builder as tb
        >>> links = tb.view.LinkGroupViewV1(
        >>>     [
        >>>         tb.view.LinkGroupViewV1.Link(
        >>>             'https://any.com/useful/url/1',
        >>>             'Example1',
        >>>         ),
        >>>         tb.view.LinkGroupViewV1.Link(
        >>>             'https://any.com/useful/url/2',
        >>>             'Example2',
        >>>         ),
        >>>     ]
        >>> )
        ...
    """

    class Link(BaseTemplate):
        """A link for the `LinkGroupViewV1`.

        Attributes:
            url: A URL.
            content: A link text.
            theme: The appearance of the link.
                If `theme` is omitted, the link is displayed as an underlined text.
                If `theme` is set to `primary`, a button is displayed.
        """

        url: base_component_or(str)
        content: base_component_or(str)
        theme: base_component_or(str) = attribute(kw_only=True)

    links: base_component_or(List[base_component_or(Link)], 'ListBaseComponentOrLink')  # noqa: F821


@inherit_docstrings
class ListViewV1(BaseViewV1, spec_value=ComponentType.VIEW_LIST):
    """A list of components.

    For more information, see [view.list](https://toloka.ai/docs/template-builder/reference/view.list).

    Attributes:
        items: List items.
        direction: The direction of the list:
            * `vertical`
            * `horizontal`

            Default value: `vertical`.
        size: A spacing between list items:
            `m` — The default spacing.
            `s` — Narrower spacing.
    """

    items: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821
    direction: base_component_or(ListDirection) = attribute(kw_only=True)
    size: base_component_or(ListSize) = attribute(kw_only=True)


@inherit_docstrings
class MarkdownViewV1(BaseViewV1, spec_value=ComponentType.VIEW_MARKDOWN):
    """A component for formatting Markdown.

    The Markdown content must not contain line breaks. To insert them, place `\n` in the text.
    Straight quotation marks must be escaped: `\"`.

    For more information, see [view.markdown](https://toloka.ai/docs/template-builder/reference/view.markdown).

    Attributes:
        content: A text with Markdown.

    Example:
        Adding a title and description using Markdown.

        >>> import toloka.client.project.template_builder as tb
        >>> header = tb.view.MarkdownViewV1('# Some Header:\n---\nSome detailed description')
        ...
    """

    content: base_component_or(Any)


@inherit_docstrings
class TextViewV1(BaseViewV1, spec_value=ComponentType.VIEW_TEXT):
    """A view for displaying a text.

    For more information, see [view.text](https://toloka.ai/docs/template-builder/reference/view.text).

    Attributes:
        content: The text. To insert a new line, use `\n`.

    Example:
        >>> import toloka.client.project.template_builder as tb
        >>> text_view = tb.view.TextViewV1(tb.data.InputData('input_field_name'), label='My label:')
        ...
    """

    content: base_component_or(Any)


@inherit_docstrings
class VideoViewV1(BaseViewV1, spec_value=ComponentType.VIEW_VIDEO):
    """A video player.

    For more information, see [view.video](https://toloka.ai/docs/template-builder/reference/view.video).

    Attributes:
        url: The video URL.
        full_height: If `True`, the component takes up all the vertical free space.
            Note, that the minimum height required by the component is 400 pixels.
        max_width: The maximum width of the component in pixels.
        min_width: The minimum width of the component in pixels. It takes priority over the `max_width`.
        ratio: A list with the aspect ratio of the component. Specify the relative width first and then the relative height.
    """

    url: base_component_or(Any)
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    max_width: base_component_or(float) = attribute(origin='maxWidth', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821


@inherit_docstrings
class MapViewV1(BaseViewV1, spec_value=ComponentType.VIEW_MAP):
    """A component for displaying a map.

    For more information, see [view.map](https://toloka.ai/docs/template-builder/reference/view.map).

    Attributes:
        center: The coordinates of the map center. You can use:
            * A string. For example, `29.748713,-95.404287`
            * The [LocationData](toloka.client.project.template_builder.data.LocationData.md) to set Toloker's current position.
        markers: A list of map markers.
        polygons: A list of areas highlighted on the map.
        zoom: An initial map scale. Use values from 0 to 19. Higher values zoom in the map.
    """

    class Marker(BaseTemplate):
        """A map marker.

        Attributes:
            position: The coordinates of the marker. You can use:
                * A string. For example, `29.748713,-95.404287`.
                * The [LocationData](toloka.client.project.template_builder.data.LocationData.md) to set Toloker's current position.
            color: The marker color. Use the hexadecimal values preceded by the `#`. For example, `#f00` makes the marker red.
            label: A marker label.
        """
        position: base_component_or(str)
        color: base_component_or(str) = attribute(kw_only=True)
        label: base_component_or(str) = attribute(kw_only=True)

    class Polygon(BaseTemplate):
        """A map polygon.

        Attributes:
            points: A list of polygon coordinates.
                Specify the coordinates in the string format, for example `29.748713,-95.404287`.
            color: The color of the polygon. Use the hexadecimal values preceded by the `#`. For example, `#f00` makes the polygon red.
        """
        points: base_component_or(List[base_component_or(str)], 'ListBaseComponentOrStr')  # noqa: F821
        color: base_component_or(str) = attribute(kw_only=True)

    center: base_component_or(str)
    markers: base_component_or(List[base_component_or(Marker)], 'ListBaseComponentOrMarker') = attribute(kw_only=True)  # noqa: F821
    polygons: base_component_or(List[base_component_or(Polygon)], 'ListBaseComponentOrPolygon') = attribute(  # noqa: F821
        kw_only=True)
    zoom: base_component_or(int) = attribute(kw_only=True)

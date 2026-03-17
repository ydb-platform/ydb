__all__ = [
    'BaseLayoutV1',

    'BarsLayoutV1',
    'ColumnsLayoutV1',
    'CompareLayoutItem',
    'CompareLayoutV1',
    'SideBySideLayoutV1',
    'SidebarLayoutV1'
]
from enum import unique
from typing import List

from .base import BaseComponent, ComponentType, VersionedBaseComponentMetaclass, base_component_or, BaseTemplate
from ....util._codegen import attribute
from ....util._extendable_enum import ExtendableStrEnum
from ....util._docstrings import inherit_docstrings


class BaseLayoutV1Metaclass(VersionedBaseComponentMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        if 'validation' not in namespace:
            namespace['validation'] = attribute(kw_only=True)
            namespace.setdefault('__annotations__', {})['validation'] = BaseComponent
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BaseLayoutV1(BaseComponent, metaclass=BaseLayoutV1Metaclass):
    """A base component for layouts.

    Layout components are used for positioning elements in the interface, such as in columns or side-by-side.

    Attributes:
        validation: Validation rules.
    """

    pass


@inherit_docstrings
class BarsLayoutV1(BaseLayoutV1, spec_value=ComponentType.LAYOUT_BARS):
    """A layout with top and bottom bars.

    For more information, see [layout.bars](https://toloka.ai/docs/template-builder/reference/layout.bars).

    Attributes:
        content: The main content.
        bar_after: The bar displayed below the main content.
        bar_before: The bar displayed above the main content.
    """

    content: BaseComponent
    bar_after: BaseComponent = attribute(origin='barAfter', kw_only=True)
    bar_before: BaseComponent = attribute(origin='barBefore', kw_only=True)


@inherit_docstrings
class ColumnsLayoutV1(BaseLayoutV1, spec_value=ComponentType.LAYOUT_COLUMNS):
    """A layout with columns.

    For more information, see [layout.columns](https://toloka.ai/docs/template-builder/reference/layout.columns).

    Attributes:
        items: A list of components. Every component is placed in an individual column.
        full_height: A height mode:
           * `True` — The component occupies all available vertical space. Columns have individual scrolling.
           * `False` — The height of the component is determined by the highest column.
        min_width: The minimum width of the component when columns are used. If the component is narrower than `min_width`, then all content is shown in one column.
        ratio: A list of relative column widths.
        vertical_align: The vertical alignment of column content.
    """

    @unique
    class VerticalAlign(ExtendableStrEnum):
        """The vertical alignment of column content.

        Attributes:
            TOP: Aligning to the top of a column.
            MIDDLE: Aligning to the middle of the highest.
            BOTTOM: Aligning to the bottom of a column.
        """

        BOTTOM = 'bottom'
        MIDDLE = 'middle'
        TOP = 'top'

    items: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821
    vertical_align: base_component_or(VerticalAlign) = attribute(origin='verticalAlign', kw_only=True)


class CompareLayoutItem(BaseTemplate):
    """An item for the `CompareLayoutV1`.

    Attributes:
        content: The content of the item.
        controls: Item controls.
    """

    content: BaseComponent
    controls: BaseComponent


@inherit_docstrings
class CompareLayoutV1(BaseLayoutV1, spec_value=ComponentType.LAYOUT_COMPARE):
    """A layout for comparing several items.

    For more information, see [layout.compare](https://toloka.ai/docs/template-builder/reference/layout.compare).

    Attributes:
        common_controls: A component containing common controls.
        items: A list of items to be compared.
        min_width: The minimum width of the component in pixels. Default value: `400`.
        wide_common_controls:
            * `True` — The common controls are stretched horizontally.
            * `False` — The common controls are centered.

            Default value: `False`.
    """

    common_controls: BaseComponent = attribute(origin='commonControls')
    items: base_component_or(List[base_component_or(CompareLayoutItem)], 'ListBaseComponentOrCompareLayoutItem')  # noqa: F821
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    wide_common_controls: base_component_or(bool) = attribute(origin='wideCommonControls', kw_only=True)


@inherit_docstrings
class SideBySideLayoutV1(BaseLayoutV1, spec_value=ComponentType.LAYOUT_SIDE_BY_SIDE):
    """A layout with several blocks of the same width on a single horizontal panel.

    For more information, see [layout.side-by-side](https://toloka.ai/docs/template-builder/reference/layout.side-by-side).

    Attributes:
        controls: A component with controls.
        items: A list of blocks.
        min_item_width: The minimum width of a block, at least 400 pixels.
    """

    controls: BaseComponent
    items: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821
    min_item_width: base_component_or(float) = attribute(origin='minItemWidth', kw_only=True)


@inherit_docstrings
class SidebarLayoutV1(BaseLayoutV1, spec_value=ComponentType.LAYOUT_SIDEBAR):
    """A layout with a main content block and a panel with controls.

    The component supports modes:
        * Widescreen — The control panel is placed to the right of the main block.
        * Compact — The controls are placed under the main block and stretch to the entire width.

    For more information, see [layout.sidebar](https://toloka.ai/docs/template-builder/reference/layout.sidebar).

    Attributes:
        content: The main block.
        controls: The control panel.
        controls_width: The width in pixels of the control panel in the widescreen mode.
            Default value: `200`.
        extra_controls: An additional panel with controls. It is placed below the controls.
        min_width: The minimum width in pixels of the component in a widescreen mode.
            If the component width is less than the specified value, the interface switches to the compact mode.
            Default value: `400`.
    """

    content: BaseComponent
    controls: BaseComponent
    controls_width: base_component_or(float) = attribute(origin='controlsWidth', kw_only=True)
    extra_controls: BaseComponent = attribute(origin='extraControls', kw_only=True)
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)

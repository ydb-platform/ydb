__all__ = [
    'BasePluginV1',
    'ImageAnnotationHotkeysPluginV1',
    'TextAnnotationHotkeysPluginV1',
    'HotkeysPluginV1',
    'TriggerPluginV1',
    'TolokaPluginV1',
]

from enum import unique
from typing import List, Any


from .base import VersionedBaseComponentMetaclass, BaseComponent, ComponentType, BaseTemplate, base_component_or
from ....util._codegen import attribute, expand
from ....util._extendable_enum import ExtendableStrEnum


class BasePluginV1(BaseComponent, metaclass=VersionedBaseComponentMetaclass):
    """A base class for plugins.
    """

    pass


class ImageAnnotationHotkeysPluginV1(BasePluginV1, spec_value=ComponentType.PLUGIN_IMAGE_ANNOTATION_HOTKEYS):
    """Hotkeys for the [ImageAnnotationFieldV1](toloka.client.project.template_builder.fields.ImageAnnotationFieldV1.md) component.

    For more information, see [plugin.field.image-annotation.hotkeys](https://toloka.ai/docs/template-builder/reference/plugin.field.image-annotation.hotkeys).

    Attributes:
        cancel: A hotkey for canceling area creation.
        confirm: A hotkey for confirming area creation.
        labels: A list of hotkeys for choosing area labels.
        modes: Hotkeys for switching selection modes.
    """

    class Mode(BaseTemplate):
        """Selection mode hotkeys.

        Attributes:
            point: A hotkey for a point selection mode.
            polygon: A hotkey for a polygon selection mode.
            rectangle: A hotkey for a rectangle selection mode.
            select: A hotkey for a mode for editing selections.
        """

        point: str = attribute(kw_only=True)
        polygon: str = attribute(kw_only=True)
        rectangle: str = attribute(kw_only=True)
        select: str = attribute(kw_only=True)

    cancel: base_component_or(str) = attribute(kw_only=True)
    confirm: base_component_or(str) = attribute(kw_only=True)
    labels: base_component_or(List[str], 'ListStr') = attribute(kw_only=True)  # noqa: F821
    modes: base_component_or(Mode) = attribute(kw_only=True)

ImageAnnotationHotkeysPluginV1.__init__ = \
    expand('modes', ImageAnnotationHotkeysPluginV1.Mode)(ImageAnnotationHotkeysPluginV1.__init__)


class TextAnnotationHotkeysPluginV1(BasePluginV1, spec_value=ComponentType.PLUGIN_TEXT_ANNOTATION_HOTKEYS):
    """Hotkeys for the [TextAnnotationFieldV1](toloka.client.project.template_builder.fields.TextAnnotationFieldV1.md) component.

    For more information, see [plugin.field.text-annotation.hotkeys](https://toloka.ai/docs/template-builder/reference/plugin.field.text-annotation.hotkeys).

    Attributes:
        labels: A list of hotkeys for choosing labels.
        remove: A hotkey for clearing the label of the annotated text.
    """

    labels: base_component_or(List[str], 'ListStr')  # noqa: F821
    remove: base_component_or(str)


class HotkeysPluginV1(BasePluginV1, spec_value=ComponentType.PLUGIN_HOTKEYS):
    """Hotkeys for actions.

    You can use as hotkeys:
    * Letters
    * Numbers
    * Up and down arrows

    Choose a hotkey using a named parameter of the `HotkeysPluginV1` and assign an action to it.

    Example:
        Creating hotkeys for classification buttons.

        >>> import toloka.client.project.template_builder as tb
        >>> hot_keys_plugin = tb.HotkeysPluginV1(
        >>>     key_1=tb.SetActionV1(tb.OutputData('result'), 'cat'),
        >>>     key_2=tb.SetActionV1(tb.OutputData('result'), 'dog'),
        >>>     key_3=tb.SetActionV1(tb.OutputData('result'), 'other'),
        >>> )
        ...
    """

    key_a: base_component_or(Any) = attribute(default=None, origin='a', kw_only=True)
    key_b: base_component_or(Any) = attribute(default=None, origin='b', kw_only=True)
    key_c: base_component_or(Any) = attribute(default=None, origin='c', kw_only=True)
    key_d: base_component_or(Any) = attribute(default=None, origin='d', kw_only=True)
    key_e: base_component_or(Any) = attribute(default=None, origin='e', kw_only=True)
    key_f: base_component_or(Any) = attribute(default=None, origin='f', kw_only=True)
    key_g: base_component_or(Any) = attribute(default=None, origin='g', kw_only=True)
    key_h: base_component_or(Any) = attribute(default=None, origin='h', kw_only=True)
    key_i: base_component_or(Any) = attribute(default=None, origin='i', kw_only=True)
    key_j: base_component_or(Any) = attribute(default=None, origin='j', kw_only=True)
    key_k: base_component_or(Any) = attribute(default=None, origin='k', kw_only=True)
    key_l: base_component_or(Any) = attribute(default=None, origin='l', kw_only=True)
    key_m: base_component_or(Any) = attribute(default=None, origin='m', kw_only=True)
    key_n: base_component_or(Any) = attribute(default=None, origin='n', kw_only=True)
    key_o: base_component_or(Any) = attribute(default=None, origin='o', kw_only=True)
    key_p: base_component_or(Any) = attribute(default=None, origin='p', kw_only=True)
    key_q: base_component_or(Any) = attribute(default=None, origin='q', kw_only=True)
    key_r: base_component_or(Any) = attribute(default=None, origin='r', kw_only=True)
    key_s: base_component_or(Any) = attribute(default=None, origin='s', kw_only=True)
    key_t: base_component_or(Any) = attribute(default=None, origin='t', kw_only=True)
    key_u: base_component_or(Any) = attribute(default=None, origin='u', kw_only=True)
    key_v: base_component_or(Any) = attribute(default=None, origin='v', kw_only=True)
    key_w: base_component_or(Any) = attribute(default=None, origin='w', kw_only=True)
    key_x: base_component_or(Any) = attribute(default=None, origin='x', kw_only=True)
    key_y: base_component_or(Any) = attribute(default=None, origin='y', kw_only=True)
    key_z: base_component_or(Any) = attribute(default=None, origin='z', kw_only=True)
    key_0: base_component_or(Any) = attribute(default=None, origin='0', kw_only=True)
    key_1: base_component_or(Any) = attribute(default=None, origin='1', kw_only=True)
    key_2: base_component_or(Any) = attribute(default=None, origin='2', kw_only=True)
    key_3: base_component_or(Any) = attribute(default=None, origin='3', kw_only=True)
    key_4: base_component_or(Any) = attribute(default=None, origin='4', kw_only=True)
    key_5: base_component_or(Any) = attribute(default=None, origin='5', kw_only=True)
    key_6: base_component_or(Any) = attribute(default=None, origin='6', kw_only=True)
    key_7: base_component_or(Any) = attribute(default=None, origin='7', kw_only=True)
    key_8: base_component_or(Any) = attribute(default=None, origin='8', kw_only=True)
    key_9: base_component_or(Any) = attribute(default=None, origin='9', kw_only=True)
    key_up: base_component_or(Any) = attribute(default=None, origin='up', kw_only=True)
    key_down: base_component_or(Any) = attribute(default=None, origin='down', kw_only=True)


class TriggerPluginV1(BasePluginV1, spec_value=ComponentType.PLUGIN_TRIGGER):
    """A plugin for triggering actions when events occur.

    For more information, see [plugin.trigger](https://toloka.ai/docs/template-builder/reference/plugin.trigger).

    Attributes:
        action: An action to trigger.
        condition: A condition that must be met in order to trigger the action.
        fire_immediately: If `True` then the action is triggered immediately after the task is loaded.
        on_change_of: The data change event that triggers the action.

    Example:
        How to save Toloker's coordinates.

        >>> import toloka.client.project.template_builder as tb
        >>> coordinates_save_plugin = tb.plugins.TriggerPluginV1(
        >>>     fire_immediately=True,
        >>>     action=tb.actions.SetActionV1(
        >>>         data=tb.data.OutputData(path='toloker_coordinates'),
        >>>         payload=tb.data.LocationData()
        >>>     ),
        >>> )
        ...
    """

    action: BaseComponent = attribute(kw_only=True)
    condition: BaseComponent = attribute(kw_only=True)
    fire_immediately: base_component_or(bool) = attribute(origin='fireImmediately', kw_only=True)
    on_change_of: BaseComponent = attribute(origin='onChangeOf', kw_only=True)


class TolokaPluginV1(BasePluginV1, spec_value=ComponentType.PLUGIN_TOLOKA):
    """A plugin with extra settings for tasks in Toloka.

    For more information, see [plugin.toloka](https://toloka.ai/docs/template-builder/reference/plugin.toloka).

    Attributes:
        layout: Settings for the task appearance in Toloka.
        notifications: Notifications shown at the top of the page.

    Example:
        Setting the width of the task block on a page.

        >>> import toloka.client.project.template_builder as tb
        >>> task_width_plugin = tb.plugins.TolokaPluginV1(
        >>>     kind='scroll',
        >>>     task_width=400,
        >>> )
        ...
    """

    class TolokaPluginLayout(BaseTemplate):
        """A task block layout.
        """

        @unique
        class Kind(ExtendableStrEnum):
            """A task block layout mode.

            Attributes:
                SCROLL: All tasks from a task suite are displayed on a page. It is the default mode.
                PAGER: A single task is displayed on a page. Buttons at the bottom of the page show other tasks from a task suite.
            """

            PAGER = 'pager'
            SCROLL = 'scroll'

        kind: Kind
        task_width: base_component_or(float) = attribute(origin='taskWidth', kw_only=True)

    layout: base_component_or(TolokaPluginLayout) = attribute(factory=TolokaPluginLayout)
    notifications: base_component_or(List[BaseComponent], 'ListBaseComponent') = attribute(kw_only=True)  # noqa: F821

TolokaPluginV1.__init__ = expand('layout', TolokaPluginV1.TolokaPluginLayout)(TolokaPluginV1.__init__)

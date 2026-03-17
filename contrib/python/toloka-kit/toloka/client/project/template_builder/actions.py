__all__ = [
    'BaseActionV1',
    'BulkActionV1',
    'NotifyActionV1',
    'OpenCloseActionV1',
    'OpenLinkActionV1',
    'PlayPauseActionV1',
    'RotateActionV1',
    'SetActionV1',
    'ToggleActionV1',
]
from enum import unique
from typing import List, Any

from .base import (
    BaseTemplate,
    ComponentType,
    RefComponent,
    BaseComponent,
    VersionedBaseComponentMetaclass,
    base_component_or
)
from ....util._codegen import attribute
from ....util._extendable_enum import ExtendableStrEnum


class BaseActionV1(BaseComponent, metaclass=VersionedBaseComponentMetaclass):
    """A base class for actions.
    """

    pass


class BulkActionV1(BaseActionV1, spec_value=ComponentType.ACTION_BULK):
    """A group of actions to be called together.

    For more information, see [action.bulk](https://toloka.ai/docs/template-builder/reference/action.bulk).

    Attributes:
        payload: A list of actions.
    """

    payload: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821


class NotifyActionV1(BaseActionV1, spec_value=ComponentType.ACTION_NOTIFY):
    """The action shows a popup message.

    For more information, see [action.notify](https://toloka.ai/docs/template-builder/reference/action.notify).

    Attributes:
        payload: Popup parameters.
    """

    class Payload(BaseTemplate):
        """Popup parameters.

        Attributes:
            content: Popup content. You can assign text or other components to the `content`.
            theme: A background color.
            delay: A delay in milliseconds before showing the popup.
            duration: A duration in milliseconds of showing the popup. It includes the delay.
        """

        @unique
        class Theme(ExtendableStrEnum):
            """The background color of a popup.

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

        content: base_component_or(Any)
        theme: base_component_or(Theme)
        delay: base_component_or(float) = attribute(kw_only=True)
        duration: base_component_or(float) = attribute(kw_only=True)

    payload: base_component_or(Payload)


class OpenCloseActionV1(BaseActionV1, spec_value=ComponentType.ACTION_OPEN_CLOSE):
    """The action changes the display mode of another component.

    It can expand an [image](toloka.client.project.template_builder.view.ImageViewV1.md) to a full screen
    or collapse a [section](toloka.client.project.template_builder.view.CollapseViewV1.md).

    For more information, see [action.open-close](https://toloka.ai/docs/template-builder/reference/action.open-close).

    Attributes:
        view: References the component to perform the action with.
    """

    view: base_component_or(RefComponent)


class OpenLinkActionV1(BaseActionV1, spec_value=ComponentType.ACTION_OPEN_LINK):
    """The action opens an URL in a new browser tab.

    For more information, see [action.open-link](https://toloka.ai/docs/template-builder/reference/action.open-link).

    Attributes:
        payload: The URL.
    """

    payload: base_component_or(Any)


class PlayPauseActionV1(BaseActionV1, spec_value=ComponentType.ACTION_PLAY_PAUSE):
    """The action pauses an audio or video player or resumes it.

    For more information, see [action.play-pause](https://toloka.ai/docs/template-builder/reference/action.play-pause).

    Attributes:
        view: A reference to the audio or video player.
    """

    view: base_component_or(RefComponent)


class RotateActionV1(BaseActionV1, spec_value=ComponentType.ACTION_ROTATE):
    """The action rotates a component by 90 degrees.

    For more information, see [action.rotate](https://toloka.ai/docs/template-builder/reference/action.rotate).

    Attributes:
        view: A reference to the component.
        payload: The direction of rotation.
    """

    @unique
    class Payload(ExtendableStrEnum):
        LEFT = 'left'
        RIGHT = 'right'

    view: base_component_or(RefComponent)
    payload: base_component_or(Payload)


class SetActionV1(BaseActionV1, spec_value=ComponentType.ACTION_SET):
    """The action sets the value of a data field.

    For more information, see [action.set](https://toloka.ai/docs/template-builder/reference/action.set).

    Example:
        The [hot key](toloka.client.project.template_builder.plugins.HotkeysPluginV1.md) `1`
        fills a [text field](toloka.client.project.template_builder.fields.TextFieldV1.md) with a predefined text.
        The [RefComponent](toloka.client.project.template_builder.base.RefComponent.md) is used to reference the output data field.

        >>> from toloka.client.project.template_builder import *
        >>>
        >>> tb_config = TemplateBuilder(
        >>>     vars={'0': OutputData('result')},
        >>>     view=TextFieldV1(
        >>>         data=RefComponent('vars.0'),
        >>>         label=InputData('question'),
        >>>     ),
        >>>     plugins=[
        >>>         HotkeysPluginV1(
        >>>             key_1=SetActionV1(
        >>>                 data=RefComponent('vars.0'),
        >>>                 payload='It is not a question'
        >>>             )
        >>>         )
        >>>     ]
        >>> )
        ...

    Attributes:
        data: The data field to set.
        payload: The value.
    """

    data: base_component_or(RefComponent)
    payload: base_component_or(Any)


class ToggleActionV1(BaseActionV1, spec_value=ComponentType.ACTION_TOGGLE):
    """The action toggles the value of a boolean data field.

    For more information, see [action.toggle](https://toloka.ai/docs/template-builder/reference/action.toggle).

    Attributes:
        data: The data field.
    """

    data: base_component_or(RefComponent)

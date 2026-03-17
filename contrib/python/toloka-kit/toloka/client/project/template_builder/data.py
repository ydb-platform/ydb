__all__ = [
    'BaseData',
    'InputData',
    'InternalData',
    'LocalData',
    'LocationData',
    'OutputData',
    'RelativeData',
]
from typing import Any

from .base import BaseComponent, ComponentType, base_component_or, BaseTemplateMetaclass
from ....util._codegen import attribute
from ....util._docstrings import inherit_docstrings


class BaseDataMetaclass(BaseTemplateMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):

        if 'path' not in namespace:
            namespace['path'] = attribute()
            namespace.setdefault('__annotations__', {})['path'] = base_component_or(Any)
        if 'default' not in namespace:
            namespace['default'] = attribute()
            namespace.setdefault('__annotations__', {})['default'] = base_component_or(Any)

        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BaseData(BaseComponent, metaclass=BaseDataMetaclass):
    """A base class for data components.

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).

     Attributes:
        path: A path to a data property in a component hierarchy.
            Dots are used as separators: `path.to.some.element`.
            For an array element, specify its sequence number after a dot: `items.0`.
        default: A default data value.
            Note, that it is shown in the interface, so it might hide placeholders, for example, in text fields.
    """

    pass


@inherit_docstrings
class InputData(BaseData, spec_value=ComponentType.DATA_INPUT):
    """Input data.

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).
    """

    pass


@inherit_docstrings
class InternalData(BaseData, spec_value=ComponentType.DATA_INTERNAL):
    """Internal task data.

    Use it to store intermediate values.

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).
    """

    pass


@inherit_docstrings
class LocalData(BaseData, spec_value=ComponentType.DATA_LOCAL):
    """Component data.

    It is used in some components, like [TransformHelperV1](toloka.client.project.template_builder.helpers.TransformHelperV1.md).

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).
    """

    pass


class LocationData(BaseComponent, spec_value=ComponentType.DATA_LOCATION):
    """Device coordinates.

    Use this component with the [DistanceConditionV1](toloka.client.project.template_builder.conditions.DistanceConditionV1.md) condition.

    For more information, see [data.location](https://toloka.ai/docs/template-builder/reference/data.location/).
    """

    pass


@inherit_docstrings
class OutputData(BaseData, spec_value=ComponentType.DATA_OUTPUT):
    """Output data.

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).
    """

    pass


@inherit_docstrings
class RelativeData(BaseData, spec_value=ComponentType.DATA_RELATIVE):
    """A component for saving data in the [ListFieldV1](toloka.client.project.template_builder.fields.ListFieldV1).

    For more information, see [Working with data](https://toloka.ai/docs/template-builder/operations/work-with-data).
    """

    pass

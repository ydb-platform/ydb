__all__ = [
    'BaseConditionV1',

    'AllConditionV1',
    'AnyConditionV1',
    'DistanceConditionV1',
    'EmptyConditionV1',
    'EqualsConditionV1',
    'LinkOpenedConditionV1',
    'NotConditionV1',
    'PlayedConditionV1',
    'PlayedFullyConditionV1',
    'RequiredConditionV1',
    'SameDomainConditionV1',
    'SchemaConditionV1',
    'SubArrayConditionV1',
]
from typing import List, Any, Dict

from ....util._codegen import attribute

from .base import BaseComponent, ComponentType, VersionedBaseComponentMetaclass, base_component_or
from ....util._docstrings import inherit_docstrings


class BaseConditionV1Metaclass(VersionedBaseComponentMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        if 'hint' not in namespace:
            namespace['hint'] = attribute(kw_only=True)
            namespace.setdefault('__annotations__', {})['hint'] = base_component_or(Any)
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BaseConditionV1(BaseComponent, metaclass=BaseConditionV1Metaclass):
    """A base class for conditions.

    Attributes:
        hint: A hint that is shown if the condition is not met.
    """

    pass


@inherit_docstrings
class AllConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_ALL):
    """Checks that all nested conditions are met.

    For more information, see [condition.all](https://toloka.ai/docs/template-builder/reference/condition.all).

    Attributes:
        conditions: A list of conditions.

    Example:
        >>> import toloka.client.project.template_builder as tb
        >>> coordinates_validation = tb.conditions.AllConditionV1(
        >>>     [
        >>>         tb.conditions.RequiredConditionV1(
        >>>             tb.data.OutputData('performer_coordinates'),
        >>>             hint="Couldn't get your coordinates. Please enable geolocation.",
        >>>         ),
        >>>         tb.conditions.DistanceConditionV1(
        >>>             tb.data.LocationData(),
        >>>             tb.data.InputData('coordinates'),
        >>>             500,
        >>>             hint='You are too far from the destination coordinates.',
        >>>         ),
        >>>     ],
        >>> )
        ...
    """

    conditions: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821


@inherit_docstrings
class AnyConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_ANY):
    """Checks that at least one nested condition is met.

    For more information, see [condition.any](https://toloka.ai/docs/template-builder/reference/condition.any).

    Attributes:
        conditions: A list of conditions.
    """

    conditions: base_component_or(List[BaseComponent], 'ListBaseComponent')  # noqa: F821


@inherit_docstrings
class DistanceConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_YANDEX_DISTANCE):
    """Checks a distance between two coordinates.

    For more information, see [condition.distance](https://toloka.ai/docs/template-builder/reference/condition.distance).

    Attributes:
        from_: The first point.
        to_: The second point.
        max: The maximum distance in meters.

    Example:
        The following condition gets Toloker's [location](toloka.client.project.template_builder.data.LocationData.md)
        and checks that it is near the task location.

        >>> import toloka.client.project.template_builder as tb
        >>> distance_condition = tb.conditions.DistanceConditionV1(
        >>>     tb.data.LocationData(),
        >>>     tb.data.InputData('coordinates'),
        >>>     500,
        >>>     hint='You are too far from the destination coordinates.',
        >>> )
        ...
    """

    from_: base_component_or(str) = attribute(origin='from')
    to_: base_component_or(str) = attribute(origin='to')
    max: base_component_or(float)


@inherit_docstrings
class EmptyConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_EMPTY):
    """Checks that data is empty or undefined.

    For more information, see [condition.empty](https://toloka.ai/docs/template-builder/reference/condition.empty).

    Attributes:
        data: Data to check. If not specified, data of the parent component is checked.
    """

    data: base_component_or(Any)


@inherit_docstrings
class EqualsConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_EQUALS):
    """Checks whether two values are equal.

    For more information, see [condition.equals](https://toloka.ai/docs/template-builder/reference/condition.equals).

    Attributes:
        data: The first value. If it is not specified, then the value returned by the parent component is used.
        to: The value to compare with.
    """

    to: base_component_or(Any)
    data: base_component_or(Any)


@inherit_docstrings
class LinkOpenedConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_LINK_OPENED):
    """Checks that a Toloker clicked a link.

    For more information, see [condition.link-opened](https://toloka.ai/docs/template-builder/reference/condition.link-opened).

    Attributes:
        url: The link URL.
    """

    url: base_component_or(Any)


@inherit_docstrings
class NotConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_NOT):
    """Inverts a condition.

    For more information, see [condition.not](https://toloka.ai/docs/template-builder/reference/condition.not).

    Attributes:
        condition: The condition to invert.
    """

    condition: BaseComponent


@inherit_docstrings
class PlayedConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_PLAYED):
    """Checks that playback has started.

    For more information, see [condition.played](https://toloka.ai/docs/template-builder/reference/condition.played).
    """

    pass


@inherit_docstrings
class PlayedFullyConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_PLAYED_FULLY):
    """Checks that playback is complete.

    For more information, see [condition.played-fully](https://toloka.ai/docs/template-builder/reference/condition.played-fully).
    """

    pass


@inherit_docstrings
class RequiredConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_REQUIRED):
    """Checks that a data field is present in a response.

    For more information, see [condition.required](https://toloka.ai/docs/template-builder/reference/condition.required).

    Attributes:
        data: The data field. If it is not specified, the data of the parent component is used.

    Example:
        How to check that image is uploaded.

        >>> import toloka.client.project.template_builder as tb
        >>> image = tb.fields.MediaFileFieldV1(
        >>>     tb.data.OutputData('image'),
        >>>     tb.fields.MediaFileFieldV1.Accept(photo=True, gallery=True),
        >>>     validation=tb.conditions.RequiredConditionV1(hint='You must upload a photo.'),
        >>> )
        ...
    """

    data: base_component_or(Any)


@inherit_docstrings
class SameDomainConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_SAME_DOMAIN):
    """Checks that domains in two URLs are the same.

    For more information, see [condition.same-domain](https://toloka.ai/docs/template-builder/reference/condition.same-domain).

    Attributes:
        data: The first URL. If it is not specified, then the value returned by the parent component is used.
        original: The second URL.
    """

    data: base_component_or(Any)
    original: base_component_or(Any)


@inherit_docstrings
class SchemaConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_SCHEMA):
    """Validates data using the [JSON Schema](https://json-schema.org/learn/getting-started-step-by-step.html).

    For more information, see [condition.schema](https://toloka.ai/docs/template-builder/reference/condition.schema).

    Attributes:
        data: Data to be validated.
        schema: The schema for validating data.
    """

    data: base_component_or(Any)
    schema: Dict  # TODO: support base_component_or(Dict)


@inherit_docstrings
class SubArrayConditionV1(BaseConditionV1, spec_value=ComponentType.CONDITION_SUB_ARRAY):
    """Checks that an array is a subarray of another array.

    For more information, see [condition.sub-array](https://toloka.ai/docs/template-builder/reference/condition.sub-array).

    Attributes:
        data: The array to check.
        parent: The array to look in.
    """

    data: base_component_or(Any)
    parent: base_component_or(Any)

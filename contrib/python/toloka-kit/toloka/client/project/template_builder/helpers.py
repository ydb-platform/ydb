__all__ = [
    'BaseHelperV1',
    'ConcatArraysHelperV1',
    'Entries2ObjectHelperV1',
    'IfHelperV1',
    'JoinHelperV1',
    'Object2EntriesHelperV1',
    'ReplaceHelperV1',
    'SearchQueryHelperV1',
    'SwitchHelperV1',
    'TextTransformHelperV1',
    'TransformHelperV1',
    'TranslateHelperV1',
    'YandexDiskProxyHelperV1'
]
from enum import unique
from typing import List, Any

from .base import BaseComponent, ComponentType, BaseTemplate, VersionedBaseComponentMetaclass, base_component_or
from ....util._codegen import attribute
from ....util._extendable_enum import ExtendableStrEnum


class BaseHelperV1(BaseComponent, metaclass=VersionedBaseComponentMetaclass):
    """A base class for helpers.
    """

    pass


class ConcatArraysHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_CONCAT_ARRAYS):
    """Concatenates multiple arrays into a single array.

    For more information, see [helper.concat-arrays](https://toloka.ai/docs/template-builder/reference/helper.concat-arrays).

    Attributes:
        items: Arrays to concatenate.
    """

    items: base_component_or(List[base_component_or(Any)], 'ListBaseComponentOrAny')  # noqa: F821


class Entries2ObjectHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_ENTRIES2OBJECT):
    """Creates an object from an array of key-value pairs.

    For example,
    `[ {"key":"foo", "value":"hello"}, {"key":"bar","value":"world"} ]`
    is converted to `{ "foo": "hello", "bar": "world" }`.

    For more information, see [helper.entries2object](https://toloka.ai/docs/template-builder/reference/helper.entries2object).

    Attributes:
        entries: A source array of key-value pairs.
    """

    class Entry(BaseTemplate):
        key: base_component_or(str)
        value: base_component_or(Any)

    entries: base_component_or(List[base_component_or(Entry)], 'ListBaseComponentOrEntry')  # noqa: F821


class IfHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_IF):
    """The `if then else` operator.

    For more information, see [helper.if](https://toloka.ai/docs/template-builder/reference/helper.if).

    Attributes:
        condition: A condition to check.
        then: A component that is returned if the condition is `True`.
        else_: A component that is returned if the condition is `False`.

    Example:
        How to conditionally show a part of the interface.

        >>> import toloka.client.project.template_builder as tb
        >>> hidden_ui = tb.helpers.IfHelperV1(
        >>>     condition=tb.conditions.EqualsConditionV1(tb.data.OutputData('show_me'), 'show'),
        >>>     then=tb.view.ListViewV1([header, buttons, images]),
        >>> )
        ...
    """

    condition: BaseComponent
    then: base_component_or(Any)
    else_: base_component_or(Any) = attribute(origin='else', kw_only=True)


class JoinHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_JOIN):
    """Joins strings into a single string.

    For more information, see [helper.join](https://toloka.ai/docs/template-builder/reference/helper.join).

    Attributes:
        items: A list of strings to join.
        by: A delimiter. You can use any number of characters in the delimiter.
    """

    items: base_component_or(List[base_component_or(str)], 'ListBaseComponentOrStr')  # noqa: F821
    by: base_component_or(Any)


class Object2EntriesHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_OBJECT2ENTRIES):
    """Creates an array of key-value pairs from an object.

    For example,
    `{ "foo": "hello", "bar": "world" }` is converted to
    `[ {"key":"foo", "value":"hello"}, {"key":"bar","value":"world"} ]`.

    For more information, see [helper.object2entries](https://toloka.ai/docs/template-builder/reference/helper.object2entries).

    Attributes:
        data: An object to convert.
    """

    data: base_component_or(Any)


class ReplaceHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_REPLACE):
    """Replaces a substring in a string.

    For more information, see [helper.replace](https://toloka.ai/docs/template-builder/reference/helper.replace).

    Attributes:
        data: An input string.
        find: A substring to look for.
        replace: A substring to replace with.
    """

    data: base_component_or(Any)
    find: base_component_or(str)
    replace: base_component_or(str)


class SearchQueryHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_SEARCH_QUERY):
    """Creates a query for a search engine.

    For more information, see [helper.search-query](https://toloka.ai/docs/template-builder/reference/helper.search-query).

    Attributes:
        query: A query.
        engine: A search engine.
    """

    @unique
    class Engine(ExtendableStrEnum):
        YANDEX = 'yandex'
        GOOGLE = 'google'
        BING = 'bing'
        MAILRU = 'mail.ru'
        WIKIPEDIA = 'wikipedia'
        YANDEX_COLLECTIONS = 'yandex/collections'
        YANDEX_VIDEO = 'yandex/video'
        YANDEX_IMAGES = 'yandex/images'
        GOOGLE_IMAGES = 'google/images'
        YANDEX_NEWS = 'yandex/news'
        GOOGLE_NEWS = 'google/news'

    query: base_component_or(Any)
    engine: base_component_or(Engine)


class SwitchHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_SWITCH):
    """Chooses one variant from multiple cases.

    For more information, see [helper.switch](https://toloka.ai/docs/template-builder/reference/helper.switch).

    Attributes:
        cases: A list of cases.
            A case consists of a condition and a resulting component.
        default: A component that is returned if none of the conditions is `True`.
    """

    class Case(BaseTemplate):
        """A case for the `SwitchHelperV1`.

        Attributes:
            condition: A case condition.
            result: A component that is returned if the condition is `True`.
        """

        condition: BaseComponent
        result: base_component_or(Any)

    cases: base_component_or(List[base_component_or(Case)], 'ListBaseComponentOrCase')  # noqa: F821
    default: base_component_or(Any)


class TextTransformHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_TEXT_TRANSFORM):
    """Converts a text to uppercase, lowercase, or capitalize it.

    For more information, see [helper.text-transform](https://toloka.ai/docs/template-builder/reference/helper.text-transform).

    Attributes:
        data: A text to convert.
        transformation: A conversion mode:
            * `uppercase` — Makes all letters uppercase.
            * `lowercase` — Makes all letters lowercase.
            * `capitalize` — Capitalizes the first letter in the text, and leaves the rest lowercase. Note, that if there are several sentences, the rest of them are not capitalized.
    """

    @unique
    class Transformation(ExtendableStrEnum):
        UPPERCASE = 'uppercase'
        LOWERCASE = 'lowercase'
        CAPITALIZE = 'capitalize'

    data: base_component_or(Any)
    transformation: base_component_or(Transformation)


class TransformHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_TRANSFORM):
    """Creates a new array by transforming elements of another array.

    For example, you can create an array of `view.image` components from an array of links to images.

    For more information, see [helper.transform](https://toloka.ai/docs/template-builder/reference/helper.transform).

    Attributes:
        into: The template of an element of the new array.
            To insert values from the source array use the [LocalData](toloka.client.project.template_builder.data.LocalData.md) component with the data `path` set to `item`.
        items: The source array.
    """

    into: base_component_or(Any)
    items: base_component_or(List[base_component_or(Any)], 'ListBaseComponentOrAny')  # noqa: F821


class TranslateHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_TRANSLATE):
    """A component for translating interface elements to other languages.

    For more information, see [helper.translate](https://toloka.ai/docs/template-builder/reference/helper.translate).

    Attributes:
        key: The key of a phrase that has translations.
    """

    key: base_component_or(str)


class YandexDiskProxyHelperV1(BaseHelperV1, spec_value=ComponentType.HELPER_YANDEX_DISK_PROXY):
    """A component for downloading files from Yandex&#160;Disk.

    For more information, see [helper.proxy](https://toloka.ai/docs/template-builder/reference/helper.proxy).

    Attributes:
        path: A path to a file in the `/<Proxy name>/<File name>.<type>` format.
    """

    path: base_component_or(str)

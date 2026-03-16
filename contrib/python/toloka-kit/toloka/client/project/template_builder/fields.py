__all__ = [
    'BaseFieldV1',
    'AudioFieldV1',
    'ButtonRadioFieldV1',
    'GroupFieldOption',
    'ButtonRadioGroupFieldV1',
    'CheckboxFieldV1',
    'CheckboxGroupFieldV1',
    'DateFieldV1',
    'EmailFieldV1',
    'FileFieldV1',
    'ImageAnnotationFieldV1',
    'ListFieldV1',
    'MediaFileFieldV1',
    'NumberFieldV1',
    'PhoneNumberFieldV1',
    'RadioGroupFieldV1',
    'SelectFieldV1',
    'TextFieldV1',
    'TextAnnotationFieldV1',
    'TextareaFieldV1',
]
from enum import unique
from typing import List, Any, Dict

from .base import (
    BaseComponent,
    ListDirection,
    ListSize,
    ComponentType,
    BaseTemplate,
    VersionedBaseComponentMetaclass,
    base_component_or
)
from ....util._codegen import attribute
from ....util._extendable_enum import ExtendableStrEnum
from ....util._docstrings import inherit_docstrings


class BaseFieldV1Metaclass(VersionedBaseComponentMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        annotations = namespace.setdefault('__annotations__', {})
        if 'data' not in namespace:
            namespace['data'] = attribute()
            annotation = {'data': BaseComponent}
            annotations = {**annotation, **annotations}
        if 'hint' not in namespace:
            namespace['hint'] = attribute(kw_only=True)
            annotations['hint'] = base_component_or(Any)
        if 'label' not in namespace:
            namespace['label'] = attribute(kw_only=True)
            annotations['label'] = base_component_or(Any)
        if 'validation' not in namespace:
            namespace['validation'] = attribute(kw_only=True)
            annotations['validation'] = BaseComponent
        namespace['__annotations__'] = annotations
        return super().__new__(mcs, name, bases, namespace, **kwargs)


class BaseFieldV1(BaseComponent, metaclass=BaseFieldV1Metaclass):
    """A base class for input data fields.

    Input fields are used to get data from Tolokers.

    Attributes:
        data: A data path.
        label: A label above the component.
        hint: A hint.
        validation: Validation rules.
    """

    pass


@inherit_docstrings
class AudioFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_AUDIO):
    """A component for recording audio.

    For more information, see [field.audio](https://toloka.ai/docs/template-builder/reference/field.audio).

    Attributes:
        multiple:
            * `True` — Multiple audio files can be recorded or uploaded.
            * `False` — A single file can be recorded or uploaded.

            Default value: `False`.
    """

    multiple: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class ButtonRadioFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_BUTTON_RADIO):
    """A button to choose an answer.

    For more information, see [field.button-radio](https://toloka.ai/docs/template-builder/reference/field.button-radio).

    Attributes:
        value_to_set: A value to write to data.
    """

    value_to_set: base_component_or(Any) = attribute(origin='valueToSet')


class GroupFieldOption(BaseTemplate):
    """A single option for components with multiple options.

    Attributes:
        value: A value that is saved in data when the option is selected.
        label: An option text.
        hint: An additional description.
    """

    value: base_component_or(Any)
    label: base_component_or(Any)
    hint: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class ButtonRadioGroupFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_BUTTON_RADIO_GROUP):
    """A group of buttons for choosing one option.

    For more information, see [field.button-radio-group](https://toloka.ai/docs/template-builder/reference/field.button-radio-group).

    Attributes:
        options: A list of options.

    Example:

        >>> import toloka.client.project.template_builder as tb
        >>> classification_buttons = tb.fields.ButtonRadioGroupFieldV1(
        >>>     data=tb.data.OutputData(path='class'),
        >>>     options=[
        >>>         tb.fields.GroupFieldOption('Cat', 'cat'),
        >>>         tb.fields.GroupFieldOption('Dog', 'dog'),
        >>>     ],
        >>>     validation=tb.conditions.RequiredConditionV1(hint='Choose one of the answer options'),
        >>> )
        ...
    """

    options: base_component_or(List[base_component_or(GroupFieldOption)], 'ListBaseComponentOrGroupFieldOption')  # noqa: F821


@inherit_docstrings
class CheckboxFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_CHECKBOX):
    """A checkbox.

    For more information, see [field.checkbox](https://toloka.ai/docs/template-builder/reference/field.checkbox).

    Attributes:
        disabled: Disabling the checkbox.
        preserve_false:
            * `False` — If the checkbox is not selected then `False` is not added to the output data.
            * `True` — The output data is always present whether the checkbox is selected or not.

            Default value: `False`.
    """

    disabled: base_component_or(bool) = attribute(kw_only=True)
    preserve_false: base_component_or(bool) = attribute(origin='preserveFalse', kw_only=True)


@inherit_docstrings
class CheckboxGroupFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_CHECKBOX_GROUP):
    """A group of checkboxes.

    This component creates a dictionary in the output data. Values from `options` are used as keys in the dictionary.

    For more information, see [field.checkbox-group](https://toloka.ai/docs/template-builder/reference/field.checkbox-group).

    Attributes:
        options: A list of options.
        disabled: Disabling the checkbox group.
        preserve_false:
            * `False` — If a checkbox from the group is not selected then its key is not added to the dictionary.
            * `True` — The output dictionary contains all keys whether checkboxes are selected or not.

            Default value: `False`.
    """

    options: base_component_or(List[base_component_or(GroupFieldOption)], 'ListBaseComponentOrGroupFieldOption')  # noqa: F821
    disabled: base_component_or(bool) = attribute(kw_only=True)
    preserve_false: base_component_or(bool) = attribute(origin='preserveFalse', kw_only=True)


@inherit_docstrings
class DateFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_DATE):
    """A field for entering a date and time.

    For more information, see [field.date](https://toloka.ai/docs/template-builder/reference/field.date).

    Attributes:
        format: The format of the field:
            * `date-time` — Date and time.
            * `date` — Date only.
        block_list: A list of dates that a Toloker can't select.
        max: The latest date and time in the `YYYY-MM-DD hh:mm` format that a Toloker can select.
        min: The earliest date and time in the `YYYY-MM-DD hh:mm` format that a Toloker can select.
        placeholder: A text that is shown when no date is entered.
    """

    format: base_component_or(Any)
    block_list: base_component_or(List[base_component_or(Any)], 'ListBaseComponentOrAny') = attribute(  # noqa: F821
        origin='blockList',
        kw_only=True
    )
    max: base_component_or(Any) = attribute(kw_only=True)
    min: base_component_or(Any) = attribute(kw_only=True)
    placeholder: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class EmailFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_EMAIL):
    """A field for entering an email address.

    For more information, see [field.email](https://toloka.ai/docs/template-builder/reference/field.email).

    Attributes:
        placeholder: A text that is shown when no address is entered.
    """

    placeholder: Any = attribute(kw_only=True)


@inherit_docstrings
class FileFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_FILE):
    """A component for uploading files.

    For more information, see [field.file](https://toloka.ai/docs/template-builder/reference/field.file).

    Attributes:
        accept: A list of file types that can be uploaded.
            Use [MIME types](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types).
            For example, `image/jpeg`.
            By default, you can upload any files.
        multiple:
            * `True` — Multiple files can be uploaded.
            * `False` — A single file can be uploaded.

            Default value: `False`.
    """

    accept: base_component_or(List[base_component_or(str)], 'ListBaseComponentOrStr')  # noqa: F821
    multiple: base_component_or(bool) = attribute(kw_only=True)


@inherit_docstrings
class ImageAnnotationFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_IMAGE_ANNOTATION):
    """A component for annotating areas in an image.

    For more information, see [field.image-annotation](https://toloka.ai/docs/template-builder/reference/field.image-annotation).

    Attributes:
        image: The URL of the image.
        disabled: Disabling the component:
            * `False` — Annotating is allowed.
            * `True` — Annotating is disabled.

            Default value: `False`.
        full_height: If `True`, the component takes up all the vertical free space.
            Note, that the minimum height required by the component is 400 pixels.
        labels: Labels used to classify image areas.
        min_width: The minimum width of the component in pixels.
        ratio: A list with the aspect ratio of the component. Specify the relative width and then the height.
            This setting is not used if `full_height=True`.
        shapes: Area selection modes: `point`, `polygon`, and `rectangle`.
            Use mode names as keys in the `shapes` dictionary. Modes set to `True` are available in the component.
            By default, all modes are available.
    """

    class Label(BaseTemplate):
        """An image area class.

        Attributes:
            label: A text on a button for selecting the area class.
            value: A value used in output data.
        """

        label: base_component_or(str)
        value: base_component_or(str)

    @unique
    class Shape(ExtendableStrEnum):
        POINT = 'point'
        POLYGON = 'polygon'
        RECTANGLE = 'rectangle'

    image: base_component_or(str)
    disabled: base_component_or(bool) = attribute(kw_only=True)
    full_height: base_component_or(bool) = attribute(origin='fullHeight', kw_only=True)
    labels: base_component_or(List[base_component_or(Label)], 'ListBaseComponentOrLabel') = attribute(kw_only=True)  # noqa: F821
    min_width: base_component_or(float) = attribute(origin='minWidth', kw_only=True)
    ratio: base_component_or(List[base_component_or(float)], 'ListBaseComponentOrFloat') = attribute(kw_only=True)  # noqa: F821
    shapes: base_component_or(Dict[base_component_or(Shape), base_component_or(bool)],
                              'DictBaseComponentOrShapeBaseComponentOrBool') = attribute(kw_only=True)  # noqa: F821


@inherit_docstrings
class ListFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_LIST):
    """A component that allows a Toloker to add and remove list items, such as text fields.

    Use RelativeData(toloka.client.project.template_builder.data.RelativeData.md) in list items,
    otherwise all list items will change the same data.

    For more information, see [field.list](https://toloka.ai/docs/template-builder/reference/field.list).

    Attributes:
        render: A template for the list item.
        button_label: A text on a button that adds the list item.
        direction: The direction of the list:
            * `horizontal`
            * `vertical`
        editable:
            * `True` — A Toloker can add or remove list items.
            * `False` — The list can't be changed.

            Default value: `True`.
        max_length: The maximum number of list items.
        size: The distance between list items:
            * `s` — Small.
            * `m` — Medium.

            Default value: `m`.
    """

    render: BaseComponent
    button_label: base_component_or(Any) = attribute(kw_only=True)
    direction: base_component_or(ListDirection) = attribute(kw_only=True)
    editable: base_component_or(bool) = attribute(kw_only=True)
    max_length: base_component_or(float) = attribute(kw_only=True)
    size: base_component_or(ListSize) = attribute(kw_only=True)


@inherit_docstrings
class MediaFileFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_MEDIA_FILE):
    """A component for uploading media files.

    For more information, see [field.media-file](https://toloka.ai/docs/template-builder/reference/field.media-file).

    Attributes:
        accept: Selecting file sources. Every source adds an upload button.
        multiple:
            * `True` — Multiple files can be uploaded.
            * `False` — A single file can be uploaded.

            Default value: `False`.

    Example:
        A component for uploading an image or taking a photo.

        >>> import toloka.client.project.template_builder as tb
        >>> image_loader = tb.fields.MediaFileFieldV1(
        >>>     label='Upload a photo',
        >>>     data=tb.data.OutputData(path='image'),
        >>>     validation=tb.conditions.RequiredConditionV1(),
        >>>     accept=tb.fields.MediaFileFieldV1.Accept(photo=True, gallery=True),
        >>>     multiple=False
        >>> )
        ...
    """

    class Accept(BaseTemplate):
        """A choice of buttons for uploading media files from different sources.

        Attributes:
            file_system: Files from a file manager.
            gallery: Files from a gallery.
            photo: Taking photos.
            video: Taking videos.
        """

        file_system: base_component_or(bool) = attribute(origin='fileSystem', kw_only=True)
        gallery: base_component_or(bool) = attribute(kw_only=True)
        photo: base_component_or(bool) = attribute(kw_only=True)
        video: base_component_or(bool) = attribute(kw_only=True)

    accept: base_component_or(Accept)
    multiple: base_component_or(bool) = attribute(kw_only=True)


@inherit_docstrings
class NumberFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_NUMBER):
    """A field for entering a number.

    For more information, see [field.number](https://toloka.ai/docs/template-builder/reference/field.number).

    Attributes:
        maximum: The maximum number that can be entered.
        minimum: The minimum number that can be entered.
        placeholder: A text that is shown if no number is entered.
    """

    maximum: base_component_or(int) = attribute(kw_only=True)
    minimum: base_component_or(int) = attribute(kw_only=True)
    placeholder: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class PhoneNumberFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_PHONE_NUMBER):
    """A field for entering a phone number.

    For more information, see [field.phone-number](https://toloka.ai/docs/template-builder/reference/field.phone-number).

    Attributes:
        placeholder: A text that is shown if no phone number is entered.
    """

    placeholder: base_component_or(str) = attribute(kw_only=True)


@inherit_docstrings
class RadioGroupFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_RADIO_GROUP):
    """A component for selecting one value out of several options.

    For more information, see [field.radio-group](https://toloka.ai/docs/template-builder/reference/field.radio-group).

    Attributes:
        options: A list of options.
        disabled: Disabling the component:
            * `False` — Selecting an option is allowed.
            * `True` — Selecting an option is disabled.

            Default value: `False`.

    Example:
        >>> import toloka.client.project.template_builder as tb
        >>> radio_group_field = tb.fields.RadioGroupFieldV1(
        >>>     data=tb.data.OutputData(path='result'),
        >>>     options=[
        >>>         tb.fields.GroupFieldOption('Cat', 'cat'),
        >>>         tb.fields.GroupFieldOption('Dog', 'dog'),
        >>>     ],
        >>>     validation=tb.conditions.RequiredConditionV1()
        >>> )
        ...
    """

    options: base_component_or(List[base_component_or(GroupFieldOption)], 'ListBaseComponentOrGroupFieldOption')  # noqa: F821
    disabled: base_component_or(bool) = attribute(kw_only=True)


@inherit_docstrings
class SelectFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_SELECT):
    """A field for selecting from a drop-down list of options.

    For more information, see [field.select](https://toloka.ai/docs/template-builder/reference/field.select).

    Attributes:
        options: A list of options.
        placeholder: A text that is shown if no option is selected.
    """

    class Option(BaseTemplate):
        """An option.

        Attributes:
            label: An option text.
            value: A value that is saved in data when the option is selected.
        """

        label: base_component_or(Any)
        value: base_component_or(Any)

    options: base_component_or(List[base_component_or(Option)], 'ListBaseComponentOrOption')  # noqa: F821
    placeholder: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class TextFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_TEXT):
    """A field for entering a single text line.

    For more information, see [field.text](https://toloka.ai/docs/template-builder/reference/field.text).

    Attributes:
        disabled: Disabling the field:
            * `True` — A Toloker can't enter a text in the field.
            * `False` — Editing the field is allowed.

            Default value: `False`.
        placeholder: A text that is shown if no value is entered.
    """

    disabled: base_component_or(bool) = attribute(kw_only=True)
    placeholder: base_component_or(Any) = attribute(kw_only=True)


@inherit_docstrings
class TextAnnotationFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_TEXT_ANNOTATION):
    """A component for text annotation.

    For more information, see [field.text-annotation](https://toloka.ai/docs/template-builder/reference/field.text-annotation).

    Attributes:
        adjust: If `adjust` is set to `words`, entire words are selected and annotated.
            If `adjust` is omitted, any part of a text can be selected.
        content: A text for annotation.
        disabled: Disabling the component.
            Default value: `False`.
        labels: A list of annotation categories.
    """

    class Label(BaseTemplate):
        """An annotation category.

        Attributes:
            label: A category name.
            value: A value that is saved in data.
        """

        label: base_component_or(str)
        value: base_component_or(str)

    adjust: base_component_or(str) = attribute(kw_only=True)
    content: base_component_or(str) = attribute(kw_only=True)
    disabled: base_component_or(bool) = attribute(kw_only=True)
    labels: base_component_or(List[base_component_or(Label)], 'ListBaseComponentOrLabel') = attribute(kw_only=True)  # noqa: F821


@inherit_docstrings
class TextareaFieldV1(BaseFieldV1, spec_value=ComponentType.FIELD_TEXTAREA):
    """A field for entering multiline text.

    For more information, see [field.textarea](https://toloka.ai/docs/template-builder/reference/field.textarea).

    Attributes:
        disabled:
            * `False` — A Toloker can edit the field.
            * `True` — The field is disabled.

            Default value: `False`.
        placeholder: A text that is shown if the field is empty.
        resizable:
            * `True` — A Toloker can change the height of the field.
            * `False` — The field is not resizable.

            Default value: `True`.
        rows: The height of the field in text lines.
    """

    disabled: base_component_or(bool) = attribute(kw_only=True)
    placeholder: base_component_or(Any) = attribute(kw_only=True)
    resizable: base_component_or(bool) = attribute(kw_only=True)
    rows: base_component_or(float) = attribute(kw_only=True)

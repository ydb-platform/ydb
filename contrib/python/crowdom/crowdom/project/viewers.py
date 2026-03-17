from collections import UserDict
from dataclasses import dataclass
from functools import partial
from typing import List, Union, Any, Tuple, Type, Callable, Optional

import toloka.client.project.template_builder as tb

from ..base import (
    Class,
    ClassMeta,
    Label,
    LocalizedString,
    Object,
    BinaryEvaluation,
    EvaluationMeta,
    Metadata,
    ImageAnnotation,
    CLASS_OBJ_FIELD,
    CLASS_TASK_FIELD,
    EVALUATION_TASK_FIELD,
    TextFormat,
    Title,
    ConditionContext,
    AvailableLabels,
    SingleLabel,
    LabelsDisplayType,
    create_input_label_as_text_if,
)
from ..objects import Audio, Text, Image, Video, TextValidation


def gen_hotkey_plugin(key: Any, where: tb.BaseComponent, what: Any) -> tb.HotkeysPluginV1:
    return tb.plugins.HotkeysPluginV1(**{f'key_{key}': tb.SetActionV1(data=where, payload=what)})


def gen_radio_group_hotkeys(
    cls: Type[Label],
    output_path: str = CLASS_TASK_FIELD,
    start_key: int = 0,
) -> List[tb.plugins.HotkeysPluginV1]:
    return [
        gen_hotkey_plugin(i + start_key, tb.OutputData(path=output_path), what=value)
        for i, value in enumerate(cls.possible_values())
        if i + start_key < 10  # only 9 digits for hotkeys
    ]


def gen_text_view(
    content: Union[str, tb.InputData, tb.OutputData],
    text_format: TextFormat,
) -> Union[tb.TextViewV1, tb.MarkdownViewV1]:
    if text_format == TextFormat.MARKDOWN:
        return tb.MarkdownViewV1(content=content)
    else:
        return tb.TextViewV1(content=content)


def gen_text_output_view(
    data: tb.OutputData,
    validation: Optional[TextValidation],
    placeholder: Optional[LocalizedString],
    lang: str,
) -> tb.TextareaFieldV1:
    tb_validation = None
    tb_placeholder = None

    if placeholder:
        tb_placeholder = placeholder[lang]

    if validation:
        tb_validation = tb.SchemaConditionV1(
            schema={
                'type': 'string',
                'pattern': validation.regex[lang],
            },
            hint=validation.hint[lang],
        )
    return tb.TextareaFieldV1(data=data, placeholder=tb_placeholder, validation=tb_validation)


def gen_label_view(
    *args: ...,
    obj_meta: Union[ClassMeta, EvaluationMeta],
    lang: str,
    condition_context: ConditionContext,
    is_output: bool = False,
    **kwargs: ...,
) -> Union[tb.RadioGroupFieldV1, tb.SelectFieldV1, tb.ListViewV1, tb.IfHelperV1]:
    values_labels = list(zip(obj_meta.type.possible_values(), obj_meta.type.get_display_labels()))

    if not isinstance(obj_meta, ClassMeta) or not obj_meta.available_labels:
        return _gen_label_view(
            *args,
            obj_meta=obj_meta,
            values_labels=values_labels,
            lang=lang,
            condition_context=condition_context,
            is_output=is_output,
            **kwargs,
        )

    def labels_to_view(labels: AvailableLabels) -> Union[tb.RadioGroupFieldV1, tb.SelectFieldV1, tb.ListViewV1]:
        filtered_values = [label.value for label in labels.labels]
        filtered = [(value, display_label) for value, display_label in values_labels if value in filtered_values]
        return _gen_label_view(
            *args,
            obj_meta=obj_meta,
            values_labels=filtered,
            lang=lang,
            condition_context=condition_context,
            is_output=is_output,
            **kwargs,
        )

    return obj_meta.available_labels.to_toloka_if(labels_to_view, condition_context)


def _gen_label_view(
    *args: ...,
    obj_meta: Union[ClassMeta, EvaluationMeta],
    values_labels: List[Tuple[Any, LocalizedString]],
    lang: str,
    condition_context: ConditionContext,
    is_output: bool = False,
    **kwargs: ...,
) -> Union[tb.RadioGroupFieldV1, tb.SelectFieldV1, tb.ListViewV1]:
    radio_group_view = tb.RadioGroupFieldV1(
        *args,
        options=[tb.GroupFieldOption(label=label[lang], value=value) for value, label in values_labels],
        disabled=not is_output,
        **kwargs,
    )
    select_field_view = tb.SelectFieldV1(
        *args,
        options=[tb.SelectFieldV1.Option(label=label[lang], value=value) for value, label in values_labels],
        **kwargs,
    )

    many_labels = len(values_labels) > 5

    if is_output:
        if many_labels:
            options_view = select_field_view
        else:
            options_view = radio_group_view
    else:
        as_text = isinstance(obj_meta, ClassMeta) and obj_meta.input_display_type == LabelsDisplayType.MONO
        if as_text or many_labels:

            def text_label_view(label: SingleLabel) -> tb.TextViewV1:
                assert isinstance(label.label, Class)
                return gen_text_view(content=label.label.get_label()[lang], text_format=obj_meta.text_format)

            options_view = create_input_label_as_text_if(
                obj_meta.name, obj_meta.type.possible_instances()
            ).to_toloka_if(
                view_gen=text_label_view,
                condition_context=condition_context,
            )
        else:
            options_view = radio_group_view

    return add_title(options_view, obj_meta.title, lang)


def add_title(view: Any, title: Title, lang: str) -> Union[Any, tb.ListViewV1]:
    if title:
        return tb.GroupViewV1(
            content=tb.ListViewV1(items=[gen_text_view(content=title.text[lang], text_format=title.format), view])
        )
    return view


@dataclass(frozen=True)
class ObjectInfo:
    obj_field: str
    task_field: str
    view_field: str
    input_viewer: Union[Type[tb.view.BaseViewV1], Callable[..., tb.fields.BaseFieldV1]]
    output_viewer: Union[Type[tb.fields.BaseFieldV1], Callable[..., tb.fields.BaseFieldV1]]
    input_validation: Optional[Callable[..., tb.conditions.BaseConditionV1]] = None
    output_validation: Optional[Callable[..., tb.conditions.BaseConditionV1]] = None
    hotkeys: Callable[..., List[tb.plugins.HotkeysPluginV1]] = lambda *args, **kwargs: []


class ViewInfo(UserDict):
    def __getitem__(self, item: Type[Object]) -> ObjectInfo:
        if isinstance(item, type) and issubclass(item, Class):
            return self.data[Class]
        return self.data[item]


def option_choice_required(data: tb.data.BaseData, lang: str) -> tb.RequiredConditionV1:
    return tb.RequiredConditionV1(
        data=data,
        hint=LocalizedString(
            {
                'EN': 'Choose one of the options',
                'RU': 'Выберите один из вариантов',
                'TR': 'Seçeneklerden birini seçin',
                'KK': 'Опциялардың бірін таңдаңыз',
            }
        )[lang],
    )


def image_annotation_required(data: tb.data.BaseData, lang: str) -> tb.RequiredConditionV1:
    return tb.RequiredConditionV1(
        data=data,
        hint=LocalizedString(
            {
                'EN': 'Select at least one area',
                'RU': 'Выделите хотя бы одну область',
                # todo: TR, KK
            }
        )[lang],
    )


def raise_assert_on_output_metadata(*args, **kwargs):
    assert False, 'Metadata objects can only be used as inputs'


object_to_info = ViewInfo(
    {  # Dict[Type[Object], ObjectInfo]
        Audio: ObjectInfo(
            obj_field='url',
            task_field='audio',
            view_field='url',
            input_viewer=tb.AudioViewV1,
            input_validation=lambda *args, **kwargs: tb.PlayedFullyConditionV1(),
            output_viewer=tb.AudioFieldV1,
        ),
        Text: ObjectInfo(
            obj_field='text',
            task_field='text',
            view_field='content',
            input_viewer=gen_text_view,
            output_viewer=gen_text_output_view,
        ),  # TextField may be more suitable in some cases
        Image: ObjectInfo(
            obj_field='url',
            task_field='image',
            view_field='url',
            input_viewer=tb.ImageViewV1,
            output_viewer=lambda *args, **kwargs: tb.MediaFileFieldV1(
                *args, accept=tb.fields.MediaFileFieldV1.Accept(photo=True), **kwargs
            ),
        ),
        Video: ObjectInfo(
            obj_field='url',
            task_field='video',
            view_field='url',
            input_viewer=tb.VideoViewV1,
            output_viewer=lambda *args, **kwargs: tb.MediaFileFieldV1(
                *args, accept=tb.fields.MediaFileFieldV1.Accept(video=True), **kwargs
            ),
        ),
        BinaryEvaluation: ObjectInfo(
            obj_field='ok',
            task_field=EVALUATION_TASK_FIELD,
            view_field='ok',
            input_viewer=gen_label_view,
            output_viewer=partial(gen_label_view, is_output=True),
            output_validation=option_choice_required,
            hotkeys=gen_radio_group_hotkeys,
        ),
        Class: ObjectInfo(
            obj_field=CLASS_OBJ_FIELD,
            task_field=CLASS_TASK_FIELD,
            view_field=CLASS_OBJ_FIELD,
            input_viewer=gen_label_view,
            output_viewer=partial(gen_label_view, is_output=True),
            output_validation=option_choice_required,
            hotkeys=gen_radio_group_hotkeys,
        ),
        Metadata: ObjectInfo(
            obj_field='metadata',
            task_field='metadata',
            view_field='metadata',
            input_viewer=lambda *args, **kwargs: tb.ListViewV1([]),
            output_viewer=raise_assert_on_output_metadata,
        ),
        ImageAnnotation: ObjectInfo(
            obj_field='data',
            task_field='image_annotation',
            view_field='data',
            input_viewer=lambda *args, **kwargs: tb.ImageAnnotationFieldV1(
                *args, disabled=True, full_height=True, **kwargs
            ),
            output_viewer=lambda *args, **kwargs: tb.ImageAnnotationFieldV1(*args, full_height=True, **kwargs),
            output_validation=image_annotation_required,  # todo: questionable
        ),
    }
)

missing_media_notification_1 = LocalizedString(
    {
        'EN': 'Before performing a task, make sure that all media elements have loaded.',
        'RU': 'Перед выполнением проверьте, что все медиа-элементы загрузились.',
        'TR': 'Çalıştırmadan önce tüm medya öğelerinin yüklendiğini kontrol edin.',
        'KK': 'Іске қоспас бұрын барлық медиа элементтердің жүктелгенін тексеріңіз.',
    }
)
missing_media_notification_2 = LocalizedString(
    {
        'EN': 'If at least one media element is missing, reload the page.',
        'RU': 'Если нет хотя бы одного медиа-элемента, перезагрузите страницу.',
        'TR': 'En az bir medya öğesi yoksa, sayfayı yeniden yükleyin.',
        'KK': 'Егер кем дегенде бір медиа элемент болмаса, бетті қайта жүктеңіз.',
    }
)
media_notifications = [missing_media_notification_1, missing_media_notification_2]

CHECK = LocalizedString(
    {
        'EN': 'check',
        'RU': 'проверка',
        'TR': 'kontrol',
        'KK': 'тексеру',
        'DE': 'Prüfung',
        'ES': 'verificación',
        'HE': 'בקרה',
    }
)

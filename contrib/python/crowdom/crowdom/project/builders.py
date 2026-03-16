from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Tuple, Iterable, Optional, Type

import toloka.client.project.template_builder as tb
import toloka.client as toloka

from ..base import (
    TaskFunction,
    ClassificationFunction,
    AnnotationFunction,
    SbSFunction,
    BinaryEvaluation,
    EvaluationMeta,
    Label,
    Object,
    ObjectMeta,
    LocalizedString,
    TextFormat,
    Title,
    ConditionContext,
    ClassMeta,
    ImageAnnotation,
    ImageAnnotationMeta,
)
from ..mapping import TaskMapping, ObjectMapping
from ..objects import Audio, Image, Video, Text, TextMeta
from .viewers import object_to_info, media_notifications


class Scenario(Enum):
    DEFAULT = 'default'
    EXPERT_LABELING_OF_TASKS = 'expert-task'
    EXPERT_LABELING_OF_SOLVED_TASKS = 'expert-solution'

    @property
    def project_name_suffix(self) -> LocalizedString:
        return LocalizedString(
            {
                self.EXPERT_LABELING_OF_TASKS: {'EN': 'task labeling', 'RU': 'разметка заданий'},
                self.EXPERT_LABELING_OF_SOLVED_TASKS: {'EN': 'solution labeling', 'RU': 'разметка решений'},
            }[self]
        )


@dataclass
class Builder:
    task_function: TaskFunction
    lang: str
    current_hotkey: int = 1
    task_width: float = 700.0

    condition_context: ConditionContext = field(default_factory=lambda: ConditionContext(name_to_arg_data={}))

    EXPERT_LABELING_METAS = (
        EvaluationMeta(
            BinaryEvaluation,
            '_ok',
            Title(
                text=LocalizedString(
                    {
                        'EN': 'Is the task (and the solution, if given) correct?',
                        'RU': 'Корректно ли задание (и решение, при наличии)?',
                    },
                ),
            ),
        ),
        ObjectMeta(
            Text,
            '_comment',
            Title(text=LocalizedString({'EN': 'comment', 'RU': 'комментарий'})),
            required=False,
        ),
    )

    def get_view_and_mapping(
        self,
        items: Tuple[ObjectMeta, ...],
        is_output: bool = False,
        suffix: str = '',
    ) -> Tuple[List[tb.view.BaseViewV1], List[ObjectMapping], List[tb.plugins.HotkeysPluginV1]]:
        object_views, object_mappings, hotkeys = [], [], []
        for obj_view, obj_mapping, obj_hotkeys in self.get_object_list_tb_views_and_mappings(
            items,
            is_output=is_output,
            suffix=suffix,
        ):
            object_views.append(obj_view)
            object_mappings.append(obj_mapping)
            hotkeys.extend(obj_hotkeys)
        return object_views, object_mappings, hotkeys

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.task_function.get_inputs()

    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        return self.task_function.get_outputs()

    def get_object_tb_view_and_mapping(
        self,
        obj_meta: ObjectMeta,
        is_output: bool,
        suffix: str = '',
    ) -> Tuple[tb.view.BaseViewV1, ObjectMapping, List[tb.plugins.HotkeysPluginV1]]:
        info = object_to_info[obj_meta.type]
        task_field = f'{obj_meta.name}{suffix}'
        obj_field = info.obj_field
        view_field = info.view_field

        self.condition_context.name_to_arg_data[obj_meta.name] = ConditionContext.ArgData(
            final_name=task_field,
            is_output=is_output,
        )

        obj_mapping = ObjectMapping(obj_meta=obj_meta, obj_task_fields=((obj_field, task_field),))

        hotkeys_enabled = True
        if isinstance(obj_meta, ClassMeta) and obj_meta.available_labels:
            # TODO (DATAFORGE-70): correct hotkeys for available labels
            hotkeys_enabled = False

        if is_output:
            data = tb.OutputData(path=task_field)
            validation = info.output_validation
            kwargs = {'data': data}
            viewer = info.output_viewer
            hotkeys = []
            if hotkeys_enabled:
                hotkeys = info.hotkeys(obj_meta.type, start_key=self.current_hotkey, output_path=task_field)
                self.current_hotkey += len(hotkeys)
        else:
            if issubclass(obj_meta.type, Label) or issubclass(obj_meta.type, ImageAnnotation):
                # radio group button cannot be used with InputData directly
                data = {'data': tb.InternalData(path=task_field, default=tb.InputData(path=task_field))}
            else:
                data = {view_field: tb.InputData(path=task_field)}
            validation = info.input_validation
            kwargs = {**data}
            viewer = info.input_viewer
            hotkeys = []

        if validation:
            # specifying 'data' is not always necessary, TB can determine data itself
            # but for some cases, i.e. compare layout in SbS, we have to specify data
            kwargs['validation'] = validation(data=data, lang=self.lang)

        if issubclass(obj_meta.type, Label):
            kwargs['lang'] = self.lang
            kwargs['obj_meta'] = obj_meta
            kwargs['condition_context'] = self.condition_context

        if isinstance(obj_meta, ImageAnnotationMeta):
            kwargs['shapes'] = {shape: True for shape in obj_meta.available_shapes}
            if obj_meta.labels is not None:
                kwargs['labels'] = [
                    tb.fields.ImageAnnotationFieldV1.Label(label=item.get_label()[self.lang], value=item.value)
                    for item in obj_meta.labels.possible_instances()
                ]

        if issubclass(obj_meta.type, Text):
            if is_output:
                title = None
                validation = None
                if obj_meta.title:
                    title = obj_meta.title.text
                if isinstance(obj_meta, TextMeta) and obj_meta.validation:
                    validation = obj_meta.validation

                kwargs['placeholder'] = title
                assert 'validation' not in kwargs
                kwargs['validation'] = validation
                kwargs['lang'] = self.lang

            else:
                text_format = TextFormat.PLAIN
                if isinstance(obj_meta, TextMeta):
                    text_format = obj_meta.format
                kwargs['text_format'] = text_format

        return viewer(**kwargs), obj_mapping, hotkeys

    def get_object_list_tb_views_and_mappings(
        self,
        obj_metas: Tuple[ObjectMeta, ...],
        is_output: bool,
        suffix: str = '',
    ) -> Iterable[Tuple[tb.view.BaseViewV1, ObjectMapping, List[tb.plugins.HotkeysPluginV1]]]:
        # TODO: handle task field collisions in case of multiple objects of the same type
        for obj_single_meta in obj_metas:
            yield self.get_object_tb_view_and_mapping(obj_single_meta, is_output=is_output, suffix=suffix)

    def gen_media_notifications(self, obj_metas: Tuple[ObjectMeta, ...]) -> Optional[List[tb.TextViewV1]]:
        need_media_notifications = any(meta.type in [Audio, Video, Image] for meta in obj_metas)
        if need_media_notifications:
            return [tb.TextViewV1(content=notification[self.lang]) for notification in media_notifications]
        return None

    def get_plugins(
        self,
        hotkeys: List[tb.plugins.HotkeysPluginV1],
    ) -> List[tb.plugins.BasePluginV1]:
        return hotkeys + [
            tb.TolokaPluginV1(
                'scroll', task_width=self.task_width, notifications=self.gen_media_notifications(self.get_inputs())
            )
        ]

    def combine_views(
        self,
        views: List[tb.view.BaseViewV1],
        hotkeys: List[tb.plugins.HotkeysPluginV1],
        has_expert_fields: bool = False,
    ) -> toloka.project.view_spec.ViewSpec:
        if has_expert_fields:
            views = (
                views[: -len(self.EXPERT_LABELING_METAS)]
                + [tb.MarkdownViewV1(content='---')]
                + views[-len(self.EXPERT_LABELING_METAS) :]
            )
        return toloka.project.TemplateBuilderViewSpec(
            view=tb.ListViewV1(items=views), plugins=self.get_plugins(hotkeys)
        )

    def combine_mappings(self, input_obj_mappings: List[ObjectMapping], output_obj_mappings: List[ObjectMapping]):
        return TaskMapping(input_mapping=tuple(input_obj_mappings), output_mapping=tuple(output_obj_mappings))

    def get_scenario(
        self, scenario: Scenario, for_preview: bool = False
    ) -> Tuple[toloka.project.view_spec.ViewSpec, TaskMapping]:
        expert_meta = self.EXPERT_LABELING_METAS
        if not self.task_function.has_names:
            suffixes = get_obj_type_indexes(tuple(meta.type for meta in self.get_all_arguments()))

            input_meta = tuple(
                meta.replace_name(object_to_info[meta.type].task_field + suffix)
                for meta, suffix in zip(self.get_inputs(), suffixes[: len(self.get_inputs())])
            )
            output_meta = tuple(
                meta.replace_name(object_to_info[meta.type].task_field + suffix)
                for meta, suffix in zip(self.get_outputs(), suffixes[len(self.get_inputs()) :])
            )
        else:
            input_meta, output_meta = self.get_inputs(), self.get_outputs()

        if scenario == Scenario.DEFAULT:
            input_types, output_types = input_meta, output_meta
        elif scenario == Scenario.EXPERT_LABELING_OF_TASKS:
            input_types, output_types = input_meta, output_meta + expert_meta
        elif scenario == Scenario.EXPERT_LABELING_OF_SOLVED_TASKS:
            input_types, output_types = input_meta + output_meta, expert_meta
        else:
            raise ValueError(f'unsupported scenario: {scenario}')
        if for_preview:
            input_types, output_types = input_types + output_types, ()
        input_view, input_obj_mappings, _ = self.get_view_and_mapping(input_types, is_output=False)
        output_view, output_obj_mappings, hotkeys = self.get_view_and_mapping(output_types, is_output=True)
        return self.combine_views(
            input_view + output_view, hotkeys, has_expert_fields=scenario != Scenario.DEFAULT
        ), self.combine_mappings(input_obj_mappings, output_obj_mappings)

    def get_default_scenario(self) -> Tuple[toloka.project.view_spec.ViewSpec, TaskMapping]:
        return self.get_scenario(Scenario.DEFAULT)

    def get_expert_labeling_of_tasks(self) -> Tuple[toloka.project.view_spec.ViewSpec, TaskMapping]:
        return self.get_scenario(Scenario.EXPERT_LABELING_OF_TASKS)

    def get_expert_labeling_of_solved_tasks(self) -> Tuple[toloka.project.view_spec.ViewSpec, TaskMapping]:
        return self.get_scenario(Scenario.EXPERT_LABELING_OF_SOLVED_TASKS)

    def get_all_arguments(self) -> Tuple[ObjectMeta, ...]:
        """May include other types that should be considered when giving out task_field names"""
        return self.task_function.get_all_arguments()


class ClassificationBuilder(Builder):
    task_function: ClassificationFunction


class SbSBuilder(Builder):
    task_function: SbSFunction

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.task_function.get_hints() + self.task_function.get_inputs()

    def combine_views(
        self,
        views: List[tb.view.BaseViewV1],
        hotkeys: List[tb.plugins.HotkeysPluginV1],
        has_expert_fields: bool = False,
    ) -> toloka.project.view_spec.ViewSpec:
        # we should use sbs template for visually large objects
        sbs_layout = any(input_meta.type in [Image, Video] for input_meta in self.get_inputs())
        self.task_width = 1200.0 if sbs_layout else 700.0

        hint_type_count = len(self.task_function.get_hints())
        input_type_count = len(self.task_function.get_inputs())
        non_output_type_count = hint_type_count + 2 * input_type_count

        hint_view = views[:hint_type_count]
        sbs_input_view = views[hint_type_count:non_output_type_count]
        choice_view, other_output_view = views[non_output_type_count], views[non_output_type_count + 1 :]

        views_a, views_b = sbs_input_view[:input_type_count], sbs_input_view[input_type_count:]

        def gen_sbs_objects_view(views: List[tb.view.BaseViewV1]) -> tb.view.BaseViewV1:
            if len(views) == 1:
                return views[0]
            return tb.ListViewV1(items=views)

        views_a, views_b = (gen_sbs_objects_view(views) for views in (views_a, views_b))

        if sbs_layout:
            sbs_view = tb.SideBySideLayoutV1(items=[views_a, views_b], controls=choice_view)
        else:
            common_controls = tb.ColumnsLayoutV1(
                min_width=10.0,
                vertical_align='top',
                items=[
                    tb.RadioGroupFieldV1(data=choice_view.data, disabled=choice_view.disabled, options=[option])
                    for option in choice_view.options
                ],
                validation=choice_view.validation,
            )
            sbs_view = tb.CompareLayoutV1(
                items=[tb.CompareLayoutItem(content=view) for view in (views_a, views_b)],
                wide_common_controls=True,
                common_controls=common_controls,
            )

        return super(SbSBuilder, self).combine_views(
            hint_view + [sbs_view] + other_output_view, hotkeys, has_expert_fields
        )

    def get_view_and_mapping(
        self,
        items: Tuple[ObjectMeta, ...],
        is_output: bool = False,
        suffix: str = '',
    ) -> Tuple[List[tb.view.BaseViewV1], List[ObjectMapping], List[tb.plugins.HotkeysPluginV1]]:
        if is_output:
            return super(SbSBuilder, self).get_view_and_mapping(items, is_output, suffix)

        hint_type_count = len(self.task_function.get_hints())
        input_type_count = len(self.task_function.get_inputs())

        hint_types, input_types = items[:hint_type_count], items[hint_type_count : hint_type_count + input_type_count]

        views_a, views_b, views_hint = [], [], []
        mappings_a, mappings_b, mappings_hint = [], [], []
        for views, mappings, suffix, types in (
            (views_hint, mappings_hint, '_hint', hint_types),
            (views_a, mappings_a, '_a', input_types),
            (views_b, mappings_b, '_b', input_types),
        ):
            for view, obj_mapping, _ in self.get_object_list_tb_views_and_mappings(
                types,
                is_output=False,
                suffix=suffix,
            ):
                views.append(view)
                mappings.append(obj_mapping)

        if hint_type_count + input_type_count < len(items):
            choice = items[-1]
            choice_object_view, choice_object_mapping, _ = self.get_object_tb_view_and_mapping(
                choice, is_output=is_output
            )
            choice_view = [choice_object_view]
            choice_mapping = [choice_object_mapping]
        else:
            choice_view, choice_mapping = [], []

        return (
            views_hint + views_a + views_b + choice_view,
            mappings_hint + mappings_a + mappings_b + choice_mapping,
            [],
        )


class ImageAnnotationMixin(Builder):
    task_function: AnnotationFunction

    def get_plugins(
        self,
        hotkeys: List[tb.plugins.HotkeysPluginV1],
    ) -> List[tb.plugins.BasePluginV1]:
        if not self.task_function.has_image_annotation():
            return super(ImageAnnotationMixin, self).get_plugins(hotkeys)

        return [tb.plugins.ImageAnnotationHotkeysPluginV1()] + [
            tb.TolokaPluginV1('pager', notifications=self.gen_media_notifications(self.get_inputs()))
        ]

    def combine_views(
        self,
        views: List[tb.view.BaseViewV1],
        hotkeys: List[tb.plugins.HotkeysPluginV1],
        has_expert_fields: bool = False,
    ) -> toloka.project.view_spec.ViewSpec:
        if not self.task_function.has_image_annotation():
            return super(ImageAnnotationMixin, self).combine_views(views, hotkeys, has_expert_fields)
        image_index = None
        for i, view in enumerate(views):
            if isinstance(view, tb.view.ImageViewV1):
                if image_index is None:
                    image_index = i
                else:
                    assert False, 'Found multiple input images for image segmentation task'
            elif isinstance(view, tb.fields.ImageAnnotationFieldV1):
                assert image_index is not None, 'No input image found for image segmentation task'
                view.image = views[image_index].url

        assert image_index is not None, 'No input image found for image segmentation task'
        return super(ImageAnnotationMixin, self).combine_views(
            views[:image_index] + views[image_index + 1 :], hotkeys, has_expert_fields
        )


class AnnotationBuilder(ImageAnnotationMixin):
    task_function: AnnotationFunction

    def get_scenario(
        self, scenario: Scenario, for_preview: bool = False
    ) -> Tuple[toloka.project.view_spec.ViewSpec, TaskMapping]:
        if scenario == Scenario.EXPERT_LABELING_OF_TASKS:
            evaluation_meta = self.task_function.get_evaluation_named_meta()
            self.EXPERT_LABELING_METAS = (evaluation_meta,) + self.EXPERT_LABELING_METAS
        return super(AnnotationBuilder, self).get_scenario(scenario, for_preview)


class AnnotationCheckBuilder(ImageAnnotationMixin):
    task_function: AnnotationFunction

    def get_inputs(self) -> Tuple[ObjectMeta, ...]:
        return self.task_function.get_inputs() + self.task_function.get_outputs()

    def get_outputs(self) -> Tuple[ObjectMeta, ...]:
        return (self.task_function.get_evaluation(),)


# there can be many objects of the same type in input/output of function, we need to distinguish them by indexes
def get_obj_type_indexes(types: Tuple[Type[Object], ...]) -> List[str]:
    # we should use task_fields here, because different subclasses of Class will get the same 'choice' path
    task_fields = [object_to_info[type].task_field for type in types]
    single_obj_task_fields = {t for t, cnt in Counter(task_fields).items() if cnt == 1}
    task_field_counts = defaultdict(int)
    indexes = []
    for t in task_fields:
        task_field_counts[t] += 1
        indexes.append('' if t in single_obj_task_fields else f'_{task_field_counts[t]}')
    return indexes

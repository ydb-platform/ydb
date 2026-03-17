from dataclasses import dataclass, field

import json
import toloka.client as toloka

from . import base, mapping, project


@dataclass
class PreparedTaskSpec:
    task_spec: base.TaskSpec
    lang: str
    scenario: project.Scenario = project.Scenario.DEFAULT

    view: toloka.project.view_spec.ViewSpec = field(init=False)
    task_mapping: mapping.TaskMapping = field(init=False)
    dumped_view: str = field(init=False)

    def __post_init__(self):
        if isinstance(self, AnnotationTaskSpec):
            builder = project.AnnotationBuilder
        else:
            builder = {
                base.ClassificationFunction: project.ClassificationBuilder,
                base.SbSFunction: project.SbSBuilder,
                base.AnnotationFunction: project.AnnotationCheckBuilder,
            }[type(self.function)]

        self.view, self.task_mapping = builder(task_function=self.function, lang=self.lang).get_scenario(
            self.scenario, self.for_preview()
        )
        self.dump_view()

    def dump_view(self):
        self.dumped_view = json.dumps(json.loads(self.view.unstructure()['config']), ensure_ascii=False)

    @property
    def id(self) -> str:
        return self.task_spec.id

    @property
    def function(self) -> base.TaskFunction:
        return self.task_spec.function

    @property
    def name(self) -> base.LocalizedString:
        if self.scenario == project.Scenario.DEFAULT:
            return self.task_spec.name
        return self.task_spec.name + ' (' + self.scenario.project_name_suffix + ')'

    @property
    def description(self) -> base.LocalizedString:
        return self.task_spec.description

    @property
    def instruction(self) -> base.LocalizedString:
        return self.task_spec.instruction

    def log_message(self) -> str:
        scenario = '' if self.scenario == project.Scenario.DEFAULT else f' to {self.scenario.name}'
        return f'task "{self.task_spec.id}" in {self.lang}{scenario}'

    def for_preview(self) -> bool:
        return False

    def overload_view(self, view: toloka.project.view_spec.ViewSpec):
        self.view = view
        self.dump_view()


@dataclass
class PreviewTaskSpec(PreparedTaskSpec):
    def for_preview(self) -> bool:
        return True


@dataclass
class AnnotationTaskSpec(PreparedTaskSpec):
    check: PreparedTaskSpec = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        check_spec = base.TaskSpec(
            id=f'{self.task_spec.id}_check',
            function=self.task_spec.function,
            name=self.task_spec.name + ' – ' + project.CHECK,
            description=self.description + ' – ' + project.CHECK,
            instruction=self.instruction,
        )
        self.check = PreparedTaskSpec(check_spec, self.lang, self.scenario)


@dataclass
class AnnotationPreviewTaskSpec(PreviewTaskSpec, AnnotationTaskSpec):
    def __post_init__(self):
        super().__post_init__()
        # we don't use it for previews directly, but we need transformed task mapping in check spec here
        self.check = PreviewTaskSpec(self.check.task_spec, self.lang, self.scenario)

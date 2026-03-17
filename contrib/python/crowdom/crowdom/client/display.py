from enum import Enum
import json
from typing import List, Optional, Type, Tuple, ClassVar, Dict, Union, Literal

import numpy as np
import pandas as pd

from .. import (
    base,
    classification,
    contrib,
    feedback_loop,
    mapping,
    objects,
    project,
    task_spec as spec,
)

from ..base import TaskSpec
from ..worker import NoWorker
from ..utils import DecimalEncoder


def get_spec_cls(task_function: base.TaskFunction, for_preview: bool = False) -> Type[spec.PreparedTaskSpec]:
    if type(task_function) in [base.ClassificationFunction, base.SbSFunction]:
        return spec.PreviewTaskSpec if for_preview else spec.PreparedTaskSpec
    elif isinstance(task_function, base.AnnotationFunction):
        return spec.AnnotationPreviewTaskSpec if for_preview else spec.AnnotationTaskSpec
    else:
        raise ValueError(f'Unknown task function: {task_function}')


def get_spec_cls_by_task_spec(task_spec: spec.PreparedTaskSpec) -> Type[spec.PreparedTaskSpec]:
    if isinstance(task_spec, spec.AnnotationTaskSpec):
        return spec.AnnotationPreviewTaskSpec
    return spec.PreviewTaskSpec


# template builder saves config and input fields in encoded way to URL, but it doesn't save output fields and state of
# UI, so we can create preview just for task (without solution), and there will be unnecessary config block
class TaskPreview:
    def __init__(
        self,
        input_objects: mapping.Objects,
        *,
        task_spec: Optional[spec.PreparedTaskSpec] = None,
        task_function: Optional[base.TaskFunction] = None,
        lang: Optional[str] = None,
        output_objects: Optional[mapping.Objects] = None,
    ):
        assert (
            task_spec is not None or task_function is not None and lang is not None
        ), 'Provide either task_spec or both task_function and language'
        for_preview = output_objects is not None
        if task_spec is None:
            task_spec = get_spec_cls(task_function, for_preview)(
                task_spec=TaskSpec(
                    id='id',
                    function=task_function,
                    name=base.EMPTY_STRING,
                    description=base.EMPTY_STRING,
                    instruction=None,
                ),
                lang=lang,
            )
        elif for_preview:
            task_spec = get_spec_cls_by_task_spec(task_spec)(
                task_spec=task_spec.task_spec,
                lang=task_spec.lang,
                scenario=task_spec.scenario,
            )
        self.url = self.get_url(input_objects, task_spec, output_objects)

    def get_link(self) -> str:
        return f'<a href="{self.url}" target="_blank" rel="noopener noreferrer">task preview</a>'

    def display_link(self):
        from IPython.display import HTML

        return HTML(self.get_link())

    def display(self):
        from IPython.display import HTML

        return HTML(
            f'<iframe src="{self.url}" onload="javascript:(function(o)'
            '{o.style.height=o.contentWindow.document.body.scrollHeight+"px";}(this));" '
            'style="height:800px;width:100%;border:none;overflow:hidden;"/>'
        )

    @classmethod
    def get_url(
        cls,
        input_objects: mapping.Objects,
        task_spec: spec.PreparedTaskSpec,
        output_objects: Optional[mapping.Objects] = None,
    ) -> str:
        objects_count = len(input_objects)
        if output_objects is not None:
            # for results preview we display output types as inputs, due to tb limitations
            objects_count += len(output_objects)

        if isinstance(task_spec, spec.AnnotationTaskSpec):
            if objects_count == len(task_spec.check.task_mapping.input_mapping):
                task_spec = task_spec.check
            else:
                assert objects_count == len(
                    task_spec.task_mapping.input_mapping
                ), 'incorrect number of input objects supplied'
        config, task_mapping = task_spec.dumped_view, task_spec.task_mapping
        preview_objects = input_objects
        if output_objects is not None:
            preview_objects = np.concatenate([input_objects, output_objects])
        input_values: dict = task_mapping.to_task(preview_objects).input_values

        dumped = json.dumps(
            {'config': config, 'input': json.dumps(input_values, ensure_ascii=False, cls=DecimalEncoder)},
            ensure_ascii=False,
        )
        compressed = contrib.LZString.compressToEncodedURIComponent(dumped)
        return f'https://tb.yandex.net/editor?config={compressed}'


class Results:
    row_input_objects: np.ndarray
    row_output_objects: np.ndarray

    task_spec: spec.PreparedTaskSpec

    WORKER_FIELD: ClassVar[str] = 'worker'
    PREVIEW_FIELD: ClassVar[str] = 'preview'

    def _add_task_previews(self, df: pd.DataFrame) -> pd.DataFrame:
        urls = [
            TaskPreview(task_input_objects, output_objects=task_output_objects, task_spec=self.task_spec).get_link()
            for task_input_objects, task_output_objects in zip(
                self.row_input_objects[df.index], self.row_output_objects[df.index]
            )
        ]
        df_copy = df.copy()
        df_copy[self.PREVIEW_FIELD] = urls
        return df_copy

    def html_with_task_previews(self, df: pd.DataFrame):
        from IPython.display import HTML

        return HTML(self._add_task_previews(df).to_html(escape=False))

    @staticmethod
    def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[df.astype(str).drop_duplicates().index]


class LabelingResults(Results):
    df: pd.DataFrame
    has_worker_weights: bool
    RESULT_FIELD: ClassVar[str] = 'result'
    CONFIDENCE_FIELD: ClassVar[str] = 'confidence'


# currently we only have 1 use case for combined classes, and producing M * N proba items is excessive,
# because all possible answers for each question are pooled from same 5 options
# thus, currently we revert generated answers to their original form inside Results
def get_class_fields(cls_type: Type[base.Class]) -> List[Tuple[str, str]]:
    if issubclass(cls_type, objects.CombinedAnswer):
        cls_values = list(set(cls.get_original_answer().value for cls in cls_type.possible_instances()))
    else:
        cls_values = [cls.value for cls in cls_type.possible_instances()]

    return [(cls_value, f'proba_{cls_value}') for cls_value in cls_values]


def get_labels_probas(
    labels_probas: Optional[classification.TaskLabelsProbas], cls_type: Type[base.Class]
) -> Optional[classification.TaskLabelsProbas]:
    if labels_probas is None:
        return None
    if not issubclass(cls_type, objects.CombinedAnswer):
        return {key.value: value for key, value in labels_probas.items()}

    return {key.get_original_answer().value: value for key, value in labels_probas.items()}


def get_label(raw_label: Optional[base.Label]) -> Union[base.Label, Literal[np.nan]]:
    if raw_label is None:
        return np.nan
    if not isinstance(raw_label, objects.CombinedAnswer):
        return raw_label.value

    return raw_label.get_original_answer().value


class ClassificationResults(LabelingResults):
    WORKER_WEIGHT_FIELD: ClassVar[str] = 'worker_weight'
    LABEL_FIELD: ClassVar[str] = 'label'
    OVERLAP_FIELD: ClassVar[str] = 'overlap'

    def __init__(
        self,
        input_objects: List[mapping.Objects],
        results: classification.Results,
        task_spec: spec.PreparedTaskSpec,
        worker_weights: Optional[classification.WorkerWeights] = None,
    ):
        self.task_spec = task_spec
        self.raw = results  # TODO: temporarily for select_control_tasks()

        # we will index these input objects by dataframe row indexes
        row_input_objects, row_output_objects = [], []

        input_fields = []
        for obj_mapping in task_spec.task_mapping.input_mapping:
            for _, task_field in obj_mapping.obj_task_fields:
                input_fields.append(task_field)

        self.input_fields = input_fields

        cls_type: Type[base.Class] = task_spec.task_mapping.output_mapping[0].obj_meta.type
        cls_fields = get_class_fields(cls_type)

        self.proba_fields = [cls_field for _, cls_field in cls_fields]
        self.has_worker_weights = worker_weights is not None
        rows = []
        for task_input_objects, task_result in zip(input_objects, results):
            task_row = task_spec.task_mapping.toloka_values(task_input_objects)
            del task_row[mapping.TASK_ID_FIELD]

            labels_probas, raw_labels = task_result
            labels_probas = get_labels_probas(labels_probas, cls_type)
            overlap = len(raw_labels)

            most_probable_result = classification.get_most_probable_label(labels_probas)
            if most_probable_result is not None:
                task_result, proba = most_probable_result
            else:
                assert labels_probas is None
                assert not raw_labels

                # todo: maybe confidence should also be None
                task_result, proba, labels_probas = None, 0.0, {}
                raw_labels = [(None, NoWorker())]
                if self.has_worker_weights:
                    worker_weights[None] = 0.0

            task_row[self.RESULT_FIELD] = np.nan if task_result is None else task_result
            task_row[self.CONFIDENCE_FIELD] = proba
            task_row[self.OVERLAP_FIELD] = overlap
            for cls_value, cls_field in cls_fields:
                proba = labels_probas.get(cls_value, 0.0)
                task_row[cls_field] = proba

            for raw_label, worker in raw_labels:
                worker_row = dict(task_row)
                worker_row[self.LABEL_FIELD] = get_label(raw_label)
                worker_row[self.WORKER_FIELD] = worker.id
                if self.has_worker_weights:
                    worker_row[self.WORKER_WEIGHT_FIELD] = worker_weights[worker.id]
                rows.append(worker_row)
                row_input_objects.append(task_input_objects)
                row_output_objects.append((raw_label,))

        self.df = pd.DataFrame.from_dict(rows)
        self.row_input_objects = np.array(row_input_objects)
        self.row_output_objects = np.array(row_output_objects)

    def predict(self) -> pd.DataFrame:
        return self.deduplicate(
            self.df[self.input_fields + [self.RESULT_FIELD, self.CONFIDENCE_FIELD, self.OVERLAP_FIELD]]
        )

    def predict_proba(self) -> pd.DataFrame:
        return self.deduplicate(self.df[self.input_fields + self.proba_fields + [self.OVERLAP_FIELD]])

    def worker_labels(self) -> pd.DataFrame:
        worker_labels = self.df[
            self.input_fields
            + [self.LABEL_FIELD, self.WORKER_FIELD]
            + ([self.WORKER_WEIGHT_FIELD] if self.has_worker_weights else [])
        ]
        return worker_labels[worker_labels[self.WORKER_FIELD].notnull()]


class AnnotationResults(LabelingResults):
    EVAL_FIELD: ClassVar[str] = base.EVALUATION_TASK_FIELD
    ANNOTATION_WORKER_FIELD: ClassVar[str] = 'annotator'
    CHECK_WORKER_FIELD: ClassVar[str] = 'evaluator'
    WORKER_WEIGHT_FIELD: ClassVar[str] = 'evaluator_weight'
    CHECK_OVERLAP_FIELD: ClassVar[str] = 'evaluation_overlap'
    ANNOTATION_OVERLAP_FIELD: ClassVar[str] = 'annotation_overlap'

    def __init__(
        self,
        input_objects: List[mapping.Objects],
        results: feedback_loop.Results,
        task_spec: spec.AnnotationTaskSpec,
        worker_weights: Optional[classification.WorkerWeights] = None,
    ):
        self.task_spec = task_spec
        self.raw = results  # TODO: temporarily for select_control_tasks()

        row_input_objects, row_output_objects = [], []

        input_fields = []
        for obj_mapping in task_spec.task_mapping.input_mapping:
            for _, task_field in obj_mapping.obj_task_fields:
                input_fields.append(task_field)

        self.input_fields = input_fields

        output_fields = []
        for obj_mapping in task_spec.task_mapping.output_mapping:
            for _, task_field in obj_mapping.obj_task_fields:
                output_fields.append(task_field)

        self.output_fields = output_fields

        self.has_worker_weights = worker_weights is not None

        rows = []
        for task_input_objects, solutions in zip(input_objects, results):
            inputs = task_spec.task_mapping.toloka_values(task_input_objects)
            del inputs[mapping.TASK_ID_FIELD]

            for i, solution in enumerate(solutions):
                outputs = task_spec.task_mapping.toloka_values(solution.solution, output=True)

                task_row = {**inputs, **outputs}
                task_row[self.ANNOTATION_WORKER_FIELD] = solution.worker.id

                task_row[self.RESULT_FIELD] = i == 0
                task_row[self.ANNOTATION_OVERLAP_FIELD] = len(solutions)
                if solution.evaluation is None:
                    task_row[self.CHECK_OVERLAP_FIELD] = 0
                    rows.append(task_row)
                    row_input_objects.append(task_input_objects)
                    row_output_objects.append(solution.solution)
                else:
                    assert solution.evaluation.worker_labels
                    task_row[self.CONFIDENCE_FIELD] = solution.evaluation.confidence
                    task_row[self.CHECK_OVERLAP_FIELD] = len(solution.evaluation.worker_labels)
                    for choice, worker in solution.evaluation.worker_labels:
                        worker_row = dict(task_row)
                        worker_row[self.EVAL_FIELD] = choice.ok
                        worker_row[self.CHECK_WORKER_FIELD] = worker.id
                        if self.has_worker_weights:
                            worker_row[self.WORKER_WEIGHT_FIELD] = worker_weights[worker.id]
                        rows.append(worker_row)
                        row_input_objects.append(task_input_objects)
                        row_output_objects.append(solution.solution)

        self.df = pd.DataFrame.from_dict(rows)
        self.row_input_objects = np.array(row_input_objects)
        self.row_output_objects = np.array(row_output_objects)

    def predict(self) -> pd.DataFrame:
        df = self.deduplicate(self.df[self.input_fields + self.output_fields + [self.RESULT_FIELD]])
        return df[df[self.RESULT_FIELD]].drop([self.RESULT_FIELD], axis=1)

    def predict_proba(self) -> pd.DataFrame:
        return self.deduplicate(self.df[self.input_fields + self.output_fields + [self.CONFIDENCE_FIELD]])

    def worker_labels(self) -> pd.DataFrame:
        return self.df.drop([self.RESULT_FIELD], axis=1)


class ExpertLabelingApplication(Enum):
    TRAINING = 'training'
    CONTROL_TASKS = 'control_tasks'
    ANNOTATION_CHECK_TRAINING = 'check_training'


class ExpertLabelingResults(Results):
    df: pd.DataFrame

    OK_FIELD: ClassVar[str] = '_ok'

    # todo implement this in ClassificationResults
    DURATION_FIELD: ClassVar[str] = 'duration'

    input_length: int
    output_lenght: int
    ok_index: int
    evaluation_index: Optional[int] = None
    comment_index: int
    output_names: List[str]

    def __init__(
        self,
        results: List[Tuple[Union[mapping.TaskSingleSolution, mapping.Objects], mapping.TaskMultipleSolutions]],
        task_spec: spec.PreparedTaskSpec,
        worker_id_to_name: Optional[Dict[str, str]] = None,
    ):
        self.task_spec = task_spec
        if worker_id_to_name is None:
            worker_id_to_name = {}
        # we will index these input objects by dataframe row indexes
        row_input_objects, row_output_object = [], []

        rows = []
        for _, (input_objects, solution) in results:
            if not solution:
                # expert labeling was interrupted, and this task doesn't have a solution
                continue
            [(output_objects, assignment)] = solution
            task_row = {
                **task_spec.task_mapping.toloka_values(input_objects, output=False),
                **task_spec.task_mapping.toloka_values(output_objects, output=True),
            }
            del task_row[mapping.TASK_ID_FIELD]
            task_row[self.WORKER_FIELD] = worker_id_to_name.get(assignment.user_id, assignment.user_id)
            task_row[self.DURATION_FIELD] = (assignment.submitted - assignment.created) / len(assignment.tasks)
            rows.append(task_row)
            row_input_objects.append(input_objects)
            row_output_object.append(output_objects)
        self.rows = rows
        self.df = pd.DataFrame.from_dict(rows)
        self.row_input_objects = np.array(row_input_objects)
        self.row_output_objects = np.array(row_output_object)
        function = task_spec.function

        default_spec = get_spec_cls(function)(task_spec=self.task_spec.task_spec, lang=self.task_spec.lang)

        self.input_length = len(default_spec.task_mapping.input_mapping)
        self.output_length = len(default_spec.task_mapping.output_mapping)
        ok_meta, comment_meta = project.Builder.EXPERT_LABELING_METAS

        self.output_names = [
            task_mapping.obj_task_fields[0][1] for task_mapping in self.task_spec.task_mapping.output_mapping
        ]
        self.ok_index = self.output_names.index(ok_meta.name)
        self.comment_index = self.output_names.index(comment_meta.name)
        if isinstance(function, base.AnnotationFunction):
            evaluation_name = function.get_evaluation_named_meta().name
            if evaluation_name in self.output_names:
                self.evaluation_index = self.output_names.index(evaluation_name)

    def get_results(self) -> pd.DataFrame:
        return self.df

    def get_accuracy(self):
        return self.df[self.OK_FIELD].mean()

    def get_solved_task_and_comment(
        self,
        task: mapping.Objects,
        solution: mapping.Objects,
        application: ExpertLabelingApplication,
    ) -> Tuple[mapping.TaskSingleSolution, objects.Text]:
        objs = task + solution
        if self.evaluation_index is None or application == ExpertLabelingApplication.TRAINING:
            inputs, outputs, comment = (
                objs[: self.input_length],
                objs[self.input_length : self.input_length + self.output_length],
                solution[self.comment_index],
            )
        else:
            inputs, outputs, comment = (
                objs[: self.input_length + self.output_length],
                (solution[self.evaluation_index],),
                solution[self.comment_index],
            )
        return (inputs, outputs), comment

    def suitable(self, solution: mapping.Objects, application: ExpertLabelingApplication) -> bool:
        if not solution[self.ok_index].ok:
            return False
        if application == ExpertLabelingApplication.CONTROL_TASKS:
            return True
        if solution[self.comment_index] is None or not solution[self.comment_index].text:
            return False
        if application == ExpertLabelingApplication.ANNOTATION_CHECK_TRAINING:
            return True
        if self.evaluation_index is not None:
            return solution[self.evaluation_index].ok
        return True

    def get_correct_objects(
        self,
        application: ExpertLabelingApplication = ExpertLabelingApplication.CONTROL_TASKS,
    ) -> Tuple[List[mapping.TaskSingleSolution], List[objects.Text]]:
        tasks_and_solutions = [
            (self.task_spec.task_mapping.from_task_values(row), self.task_spec.task_mapping.from_solution_values(row))
            for row in self.rows
        ]
        generated_objects = [
            self.get_solved_task_and_comment(task, solution, application)
            for task, solution in tasks_and_solutions
            if self.suitable(solution, application)
        ]
        solutions = [solution for solution, _ in generated_objects]
        texts = [text for _, text in generated_objects]
        return solutions, texts

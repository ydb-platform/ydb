from dataclasses import dataclass
from datetime import timedelta
import logging
import threading
from typing import List, Optional, Tuple, Dict, Union, Type

from lzy.api.v1 import Lzy, LocalRuntime
from lzy.whiteboards.index import DummyWhiteboardIndexClient
from lzy.types import File
import toloka.client as toloka

from .. import (
    base,
    classification,
    classification_loop,
    datasource,
    experts,
    feedback_loop,
    mapping,
    mos,
    metrics,
    pool as pool_config,
    project,
    worker,
    lzy as lzy_utils,
)
from .display import ClassificationResults, AnnotationResults
from ..pricing import get_pool_price, get_annotation_price
from ..params import ExpertParams, Params, AnnotationParams
from .task import check_project
from ..task_spec import PreparedTaskSpec, AnnotationTaskSpec
from .training import find_training_requirement
from .utils import ask, validate_objects_volume

logger = logging.getLogger(__name__)

plotter_redraw_period_seconds = 60
plotter_join_timeout_seconds = plotter_redraw_period_seconds + 30

# Also retry 409, which appears because of Toloka API bugs
if toloka.exceptions.ConflictStateApiError not in toloka.TolokaClient.EXCEPTIONS_TO_RETRY:
    toloka.TolokaClient.EXCEPTIONS_TO_RETRY += (toloka.exceptions.ConflictStateApiError,)
toloka.primitives.retry.STATUSES_TO_RETRY.add(409)


def create_toloka_client(
    token: str,
    environment: toloka.TolokaClient.Environment = toloka.TolokaClient.Environment.PRODUCTION,
) -> toloka.TolokaClient:
    return toloka.TolokaClient(token=token, environment=environment, retries=20, timeout=(60.0, 240.0))


@dataclass
class Artifacts:
    plots: File


@dataclass
class ClassificationArtifacts(Artifacts):
    wb: lzy_utils.ClassificationWhiteboard

    @property
    def results(self) -> ClassificationResults:
        return ClassificationResults(
            [objects.deserialize() for objects in self.wb.input_objects],
            self.wb.raw_results.deserialize(),
            PreparedTaskSpec(self.wb.task_spec.deserialize(), self.wb.lang),
            self.wb.worker_weights.deserialize() if self.wb.worker_weights else None,
        )


def launch(
    task_spec: PreparedTaskSpec,
    params: Params,
    input_objects: List[mapping.Objects],
    control_objects: List[mapping.TaskSingleSolution],  # assisted by us
    client: toloka.TolokaClient,
    interactive: bool = False,
    lzy: Optional[Lzy] = None,
) -> Optional[ClassificationArtifacts]:
    result = _launch(
        task_spec=task_spec,
        params=params,
        input_objects=input_objects,
        control_objects=control_objects,
        client=client,
        interactive=interactive,
        lzy=lzy,
    )

    if result is None:
        return None

    _, wb, plots = result
    return ClassificationArtifacts(plots=plots, wb=wb)


@dataclass
class MOSArtifacts(ClassificationArtifacts):
    algorithm_ci: dict


def launch_mos(
    task_spec: PreparedTaskSpec,
    params: Params,
    input_objects: List[mapping.Objects],
    client: toloka.TolokaClient,
    interactive: bool = False,
    inputs_to_metadata: Optional[Dict[mapping.Objects, mos.ObjectsMetadata]] = None,
    lzy: Optional[Lzy] = None,
) -> Optional[MOSArtifacts]:
    result = _launch(
        task_spec=task_spec,
        params=params,
        input_objects=input_objects,
        control_objects=[],
        client=client,
        interactive=interactive,
        lzy=lzy,
        loop_cls=classification_loop.MOSLoop,
        inputs_to_metadata=inputs_to_metadata,
    )

    if result is None:
        return None

    loop, wb, plots = result
    # TODO: CI may be needed in MOS whiteboard
    algorithm_ci = loop.assignment_evaluation_strategy.final_assignment_set.get_algorithm_ci()

    return MOSArtifacts(plots=plots, wb=wb, algorithm_ci=algorithm_ci)


def launch_sbs(
    task_spec: PreparedTaskSpec,
    params: Params,
    input_objects: List[mapping.Objects],
    control_objects: List[mapping.TaskSingleSolution],  # assisted by us
    client: toloka.TolokaClient,
    interactive: bool = False,
    lzy: Optional[Lzy] = None,
) -> Optional[ClassificationArtifacts]:
    result = _launch(
        task_spec=task_spec,
        params=params,
        input_objects=input_objects,
        control_objects=control_objects,
        client=client,
        interactive=interactive,
        lzy=lzy,
        loop_cls=lzy_utils.SbSLoop,
        task_function=task_spec.function,
    )

    if result is None:
        return None

    _, wb, plots = result
    return ClassificationArtifacts(plots=plots, wb=wb)


def _launch(
    task_spec: PreparedTaskSpec,
    params: Params,
    input_objects: List[mapping.Objects],
    control_objects: List[mapping.TaskSingleSolution],
    client: toloka.TolokaClient,
    interactive: bool = False,
    loop_cls: Type[classification_loop.ClassificationLoop] = classification_loop.ClassificationLoop,
    lzy: Optional[Lzy] = None,
    **kwargs,
) -> Optional[Tuple[classification_loop.ClassificationLoop, lzy_utils.ClassificationWhiteboard, File]]:
    assert task_spec.scenario == project.Scenario.DEFAULT, 'You should use this function for crowd markup only'
    assert isinstance(params.task_duration_hint, timedelta)
    assert mapping.validation_enabled
    datasource.assert_tasks_are_unique(input_objects)
    datasource.assert_tasks_are_unique(control_objects)
    # TODO: check that control objects don't contain input objects

    prj = check_project(task_spec, client)

    if not validate_objects_volume(
        input_objects,
        control_objects,
        params.task_duration_hint,
        params.pricing_config,
        interactive,
    ):
        return None

    if type(task_spec.function) not in (base.ClassificationFunction, base.SbSFunction):
        raise ValueError(f'unsupported task function: {task_spec.function}')

    if interactive:
        pool_price = get_pool_price(len(input_objects), params.pricing_config, params.overlap, display_formula=True)
        if not ask(f'run classification of {len(input_objects)} objects for {pool_price}?'):
            return None
    loop = loop_cls(
        client,
        task_spec.task_mapping,
        params.classification_loop_params,
        task_spec.lang,
        with_control_tasks=params.pricing_config.control_tasks_count > 0,
        **kwargs,
    )
    real_tasks_count = params.real_tasks_count
    control_tasks_count = params.control_tasks_count
    training_requirement = find_training_requirement(prj.id, task_spec, client, params)
    pool_cfg = pool_config.ClassificationConfig(
        project_id=prj.id,
        private_name='pool',
        reward_per_assignment=params.assignment_price,
        task_duration_hint=params.task_duration_hint,
        real_tasks_count=real_tasks_count,
        control_tasks_count=control_tasks_count,
        overlap=params.overlap.min_overlap,
        control_params=params.control,
        worker_filter=params.worker_filter,
        training_requirement=training_requirement,
        work_duration_before_vacation=params.work_duration_before_vacation,
    )

    plotter = None
    stop_event = threading.Event()

    lzy = lzy or Lzy(runtime=LocalRuntime(), whiteboard_client=DummyWhiteboardIndexClient())

    try:
        with lzy.workflow(f'{lzy_utils.crowdom_label}__{task_spec.id}', interactive=False, eager=True) as wf:
            wb_cls = {lzy_utils.SbSLoop: lzy_utils.SbSWhiteboard}.get(type(loop), lzy_utils.ClassificationWhiteboard)
            wb = wf.create_whiteboard(wb_cls, tags=[lzy_utils.crowdom_label, task_spec.id, lzy_utils.wb_version])
            wb.task_spec = lzy_utils.TaskSpec.serialize(task_spec.task_spec)
            wb.lang = task_spec.lang
            wb.input_objects = [lzy_utils.Objects.serialize(objects) for objects in input_objects]
            wb.control_objects = [lzy_utils.TaskSingleSolution.serialize(objects) for objects in control_objects]
            wb.params = lzy_utils.Params.serialize(params)
            wb.project = lzy_utils.TolokaProject.serialize(prj)

            # TODO: deencapsulation of sbs swaps logic;
            #  quick&dirty to not have outer workflow/whiteboard for sbs case
            if isinstance(loop, lzy_utils.SbSLoop):
                wb.swaps = loop.get_swaps(input_objects)
                loop.swaps = wb.swaps

            pool_id = lzy_utils.create_classification_pool(loop, control_objects, pool_cfg)
            wb.pool_id = pool_id

            plotter = metrics.ClassificationMetricsPlotter(
                toloka_client=client,
                stop_event=stop_event,
                redraw_period_seconds=plotter_redraw_period_seconds,
                task_mapping=task_spec.task_mapping,
                pool_id=pool_id,
                task_duration_hint=params.task_duration_hint,
                params=params.classification_loop_params,
                assignment_evaluation_strategy=loop.assignment_evaluation_strategy,
            )

            lzy_utils.add_input_objects(loop, input_objects, pool_id)
            wf.barrier()

            logger.info('classification has started')

            lzy_utils.run_classification_loop(loop, pool_id)
            wf.barrier()

            raw_results, worker_weights = lzy_utils.get_classification_results(loop, input_objects, pool_id)

            wb.assignments = [lzy_utils.TolokaAssignment.serialize(a) for a in client.get_assignments(pool_id=pool_id)]
            wb.pool = lzy_utils.TolokaPool.serialize(client.get_pool(pool_id))

            wb.raw_results = lzy_utils.Results.serialize(raw_results)
            wb.worker_weights = lzy_utils.WorkerWeights.serialize(worker_weights) if worker_weights else None

            return loop, wb, File(plotter.plots_image_file_name)
    except KeyboardInterrupt:
        if wb.pool_id and _is_pool_auto_close_applicable(lzy):
            client.close_pool(wb.pool_id)
        return None
    finally:
        if plotter:
            stop_event.set()
            plotter.join(timeout=plotter_join_timeout_seconds)


# tmp routine, while we didn't implement pipeline in lzy
def select_control_tasks(
    input_objects: List[mapping.Objects],
    results: Union[classification.Results, feedback_loop.Results],
    min_confidence: float,
    min_overlap: int = 1,
) -> List[mapping.TaskSingleSolution]:
    control_tasks = []
    if not isinstance(results[0][0], feedback_loop.Solution):
        for task_input_objects, (labels_probas, raw_labels) in zip(input_objects, results):
            if labels_probas is None:
                continue
            label, proba = classification.get_most_probable_label(labels_probas)
            if len(raw_labels) >= min_overlap and proba >= min_confidence:
                control_tasks.append((task_input_objects, (label,)))
        return control_tasks

    for task_input_objects, task_result in zip(input_objects, results):
        if not task_result:
            continue
        best_solution = task_result[0]
        # if we have non-trivial check_sample, there can be good annotations without evaluation - unknown confidence
        # however, in case of min_confidence == 0 we may want to receive TaskSingleSolution for _each_ input object
        if not best_solution.evaluation:
            continue
        solution, proba = best_solution.solution, best_solution.evaluation.confidence
        raw_labels = best_solution.evaluation.worker_labels
        if len(raw_labels) >= min_overlap and proba >= min_confidence:
            control_tasks.append((task_input_objects, solution))
    return control_tasks


# TODO: maybe we can create 0-cost pools in Toloka sandbox
# TODO: maybe adjust task_duration_hint considering scenarios
def launch_experts(
    task_spec: PreparedTaskSpec,
    params: ExpertParams,
    input_objects: Union[List[mapping.Objects], List[mapping.TaskSingleSolution]],
    case: experts.ExpertCase,
    client: toloka.TolokaClient,
    interactive: bool = False,
) -> List[Tuple[Union[mapping.TaskSingleSolution, mapping.Objects], mapping.TaskMultipleSolutions]]:
    assert task_spec.scenario != project.Scenario.DEFAULT, 'You should use this function for expert markup only'
    assert isinstance(params.task_duration_hint, timedelta)
    assert mapping.validation_enabled

    prj = check_project(task_spec, client)

    assignment_price = params.assignment_price
    pipeline = experts.ExpertPipeline(
        client, task_spec.function, task_spec.task_mapping, task_spec.lang, scenario=task_spec.scenario
    )

    skills = experts.get_skills(task_spec.id, task_spec.lang, client)
    assert skills, f'No suitable skills found for task spec {task_spec.id} and lang {task_spec.lang}'
    logger.debug(f'Using found skills: {skills}')

    if interactive:
        pool_price = get_pool_price(
            len(input_objects),
            params.pricing_config,
            classification_loop.StaticOverlap(overlap=1),
            display_formula=True,
        )
        if not ask(f'run expert labeling of {len(input_objects)} objects for {pool_price}?'):
            return []

    pool_cfg = pool_config.ExpertConfig(
        project_id=prj.id,
        private_name='pool',
        public_description=case.label[task_spec.lang],
        reward_per_assignment=assignment_price,
        task_duration_hint=params.task_duration_hint,
        real_tasks_count=params.pricing_config.get_tasks_count(),
        worker_filter=worker.ExpertFilter([skill for (_, skill) in skills]),
    )

    pool = pipeline.create_pool(pool_cfg)

    # todo some extra functionality will be lost here without lzy graph rearrangements - e.q. pre-/post-swaps in sbs
    try:
        pipeline.add_input_objects(pool.id, input_objects)
        logger.info('expert labeling has started')
        pipeline.loop(pool.id)
    except KeyboardInterrupt:
        client.close_pool(pool.id)

    return pipeline.get_results(pool.id, input_objects)


@dataclass
class AnnotationArtifacts(Artifacts):
    wb: lzy_utils.AnnotationWhiteboard

    @property
    def results(self) -> AnnotationResults:
        return AnnotationResults(
            [objects.deserialize() for objects in self.wb.input_objects],
            [[result.deserialize() for result in results] for results in self.wb.raw_results],
            AnnotationTaskSpec(self.wb.task_spec.deserialize(), self.wb.lang),
            self.wb.worker_weights.deserialize() if self.wb.worker_weights else None,
        )


def launch_annotation(
    task_spec: AnnotationTaskSpec,
    params: AnnotationParams,
    check_params: Params,
    input_objects: List[mapping.Objects],
    control_objects: List[mapping.TaskSingleSolution],  # assisted by us
    client: toloka.TolokaClient,
    interactive: bool = False,
    lzy: Optional[Lzy] = None,
    s3: Optional[datasource.S3] = None,
) -> Optional[AnnotationArtifacts]:
    assert task_spec.scenario == project.Scenario.DEFAULT, 'You should use this function for crowd markup only'
    assert isinstance(params.task_duration_hint, timedelta)
    assert isinstance(check_params.task_duration_hint, timedelta)
    assert mapping.validation_enabled
    datasource.assert_tasks_are_unique(input_objects)
    datasource.assert_tasks_are_unique(control_objects)
    # TODO: check that control objects don't contain input objects

    markup_prj = check_project(task_spec, client)
    check_prj = check_project(task_spec.check, client)

    if not validate_objects_volume(
        input_objects,
        [],
        params.task_duration_hint,
        params.pricing_config,
        interactive,
    ):
        return None

    if not validate_objects_volume(
        input_objects,  # we don't know number of input objects in evaluation pool in advance
        control_objects,
        check_params.task_duration_hint,
        check_params.pricing_config,
        interactive,
    ):
        return None

    for obj in task_spec.function.get_outputs():
        if obj.type.is_media():
            assert s3 is not None, 'S3 client is required for media-output tasks'

    if interactive:
        annotation_price = get_annotation_price(
            input_objects_count=len(input_objects),
            markup_config=params.pricing_config,
            check_config=check_params.pricing_config,
            markup_overlap=params.overlap,
            check_overlap=check_params.overlap,
            assignment_check_sample=params.assignment_check_sample,
            display_formula=True,
        )
        if not ask(f'run annotation of {len(input_objects)} objects for {annotation_price}?'):
            return None

    fb_loop = feedback_loop.FeedbackLoop(
        pool_input_objects=input_objects,
        markup_task_mapping=task_spec.task_mapping,
        check_task_mapping=task_spec.check.task_mapping,
        markup_params=params.feedback_loop_params,
        check_params=check_params.classification_loop_params,
        client=client,
        lang=task_spec.lang,
        s3=s3,
        model_markup=params.model,
        model_check=check_params.model,
    )

    check_training_requirement = find_training_requirement(check_prj.id, task_spec.check, client, check_params)
    markup_training_requirement = find_training_requirement(markup_prj.id, task_spec, client, params)

    check_pool_cfg = pool_config.ClassificationConfig(
        project_id=check_prj.id,
        private_name='check pool',
        reward_per_assignment=check_params.assignment_price,
        task_duration_hint=check_params.task_duration_hint,
        real_tasks_count=check_params.real_tasks_count,
        control_tasks_count=check_params.control_tasks_count,
        overlap=check_params.overlap.min_overlap,
        control_params=check_params.control,
        worker_filter=check_params.worker_filter,
        training_requirement=check_training_requirement,
        work_duration_before_vacation=check_params.work_duration_before_vacation,
    )

    markup_pool_cfg = pool_config.MarkupConfig(
        project_id=markup_prj.id,
        private_name='markup pool',
        reward_per_assignment=params.assignment_price,
        task_duration_hint=params.task_duration_hint,
        real_tasks_count=params.real_tasks_count,
        worker_filter=params.worker_filter,
        training_requirement=markup_training_requirement,
        control_params=params.control,
        first_attempt_by_model_worker=params.model is not None,
        work_duration_before_vacation=params.work_duration_before_vacation,
    )

    plotter = None
    stop_event = threading.Event()

    lzy = lzy or Lzy(runtime=LocalRuntime(), whiteboard_client=DummyWhiteboardIndexClient())

    try:
        with lzy.workflow(f'{lzy_utils.crowdom_label}__{task_spec.id}', interactive=False, eager=True) as wf:
            wb: lzy_utils.AnnotationWhiteboard = wf.create_whiteboard(lzy_utils.AnnotationWhiteboard, tags=[
                lzy_utils.crowdom_label, task_spec.id, lzy_utils.wb_version,
            ])
            wb.task_spec = lzy_utils.TaskSpec.serialize(task_spec.task_spec)
            wb.lang = task_spec.lang
            wb.input_objects = [lzy_utils.Objects.serialize(objects) for objects in input_objects]
            wb.control_objects = [lzy_utils.TaskSingleSolution.serialize(objects) for objects in control_objects]
            wb.annotation_params = lzy_utils.AnnotationParams.serialize(params)
            wb.evaluation_params = lzy_utils.Params.serialize(check_params)
            wb.annotation_project = lzy_utils.TolokaProject.serialize(markup_prj)
            wb.evaluation_project = lzy_utils.TolokaProject.serialize(check_prj)

            markup_pool_id, check_pool_id = lzy_utils.create_annotation_pools(
                fb_loop, control_objects, markup_pool_cfg, check_pool_cfg,
            )
            wb.annotation_pool_id = markup_pool_id
            wb.evaluation_pool_id = check_pool_id

            plotter = metrics.AnnotationMetricsPlotter(
                toloka_client=client,
                stop_event=stop_event,
                redraw_period_seconds=plotter_redraw_period_seconds,
                task_spec=task_spec,
                check_pool_id=check_pool_id,
                markup_pool_id=markup_pool_id,
                check_task_duration_hint=check_params.task_duration_hint,
                markup_task_duration_hint=params.task_duration_hint,
                evaluation=fb_loop.evaluation,
            )

            logger.info('annotation has started')

            lzy_utils.run_annotation_loop(fb_loop, markup_pool_id, check_pool_id)
            wf.barrier()

            raw_results, worker_weights = lzy_utils.get_annotation_results(fb_loop, markup_pool_id, check_pool_id)

            wb.annotation_assignments = [
                lzy_utils.TolokaAssignment.serialize(a) for a in client.get_assignments(pool_id=markup_pool_id)
            ]
            wb.evaluation_assignments = [
                lzy_utils.TolokaAssignment.serialize(a) for a in client.get_assignments(pool_id=check_pool_id)
            ]
            wb.annotation_pool = lzy_utils.TolokaPool.serialize(client.get_pool(markup_pool_id))
            wb.evaluation_pool = lzy_utils.TolokaPool.serialize(client.get_pool(check_pool_id))

            wb.raw_results = [
                [lzy_utils.Solution.serialize(solution) for solution in solutions]
                for solutions in raw_results
            ]
            wb.worker_weights = lzy_utils.WorkerWeights.serialize(worker_weights) if worker_weights else None

            return AnnotationArtifacts(wb=wb, plots=File(plotter.plots_image_file_name))
    except KeyboardInterrupt:
        if wb.annotation_pool_id and _is_pool_auto_close_applicable(lzy):
            client.close_pool(wb.annotation_pool_id)
        if wb.evaluation_pool_id and _is_pool_auto_close_applicable(lzy):
            client.close_pool(wb.evaluation_pool_id)
        return None
    finally:
        if plotter:
            stop_event.set()
            plotter.join(timeout=plotter_join_timeout_seconds)


# TODO: bring back pool auto closing for remote lzy launch.
#   After manual stop pools will be closed, but after relaunch from cache changed pools status will cause invalid
#   loop state, we need to discover current pool status automatically.
def _is_pool_auto_close_applicable(lzy: Lzy) -> bool:
    return isinstance(lzy.runtime, LocalRuntime)

from dataclasses import dataclass
from typing import List, Optional, Tuple

from lzy.api.v1 import op, whiteboard


from ..serialization import (
    TaskSpec,
    Objects,
    TaskSingleSolution,
    Params,
    AnnotationParams,
    TolokaProject,
    TolokaPool,
    TolokaAssignment,
    WorkerWeights,
    Solution,
)
from ... import classification, feedback_loop, mapping, pool as pool_config


# WARNING: field name changes are not backward-compatible
@dataclass
@whiteboard(name='annotation')
class Whiteboard:
    task_spec: TaskSpec
    lang: Optional[str]
    input_objects: List[Objects]
    control_objects: List[TaskSingleSolution]
    annotation_params: AnnotationParams
    evaluation_params: Params

    annotation_project: TolokaProject
    evaluation_project: TolokaProject

    annotation_pool_id: str
    evaluation_pool_id: str

    annotation_pool: TolokaPool  # with final attrs after closing
    evaluation_pool: TolokaPool  # with final attrs after closing
    annotation_assignments: List[TolokaAssignment]
    evaluation_assignments: List[TolokaAssignment]

    raw_results: List[List[Solution]]
    worker_weights: Optional[WorkerWeights]


@op(lazy_arguments=False, cache=True, version='1.0')
def create_pools(
    fb_loop: feedback_loop.FeedbackLoop,
    control_objects: List[mapping.TaskSingleSolution],
    annotation_pool_cfg: pool_config.MarkupConfig,
    evaluation_pool_cfg: pool_config.ClassificationConfig,
) -> Tuple[str, str]:
    annotation_pool, evaluation_pool = fb_loop.create_pools(control_objects, annotation_pool_cfg, evaluation_pool_cfg)
    return annotation_pool.id, evaluation_pool.id


@op(lazy_arguments=False, cache=True, version='1.0')
def run_loop(
    fb_loop: feedback_loop.FeedbackLoop,
    annotation_pool_id: str,
    evaluation_pool_id: str,
) -> None:
    fb_loop.loop(annotation_pool_id, evaluation_pool_id)


@op(lazy_arguments=False, cache=True, version='1.0')
def get_results(
    fb_loop: feedback_loop.FeedbackLoop,
    annotation_pool_id: str,
    evaluation_pool_id: str,
) -> Tuple[feedback_loop.Results, Optional[classification.WorkerWeights]]:
    return fb_loop.get_results(annotation_pool_id, evaluation_pool_id)

from dataclasses import dataclass
from typing import List, Optional, Tuple

from lzy.api.v1 import op, whiteboard

from ... import classification, classification_loop, mapping, pool as pool_config
from ..serialization import (
    TaskSpec,
    Objects,
    TaskSingleSolution,
    Params,
    TolokaProject,
    TolokaPool,
    TolokaAssignment,
    Results,
    WorkerWeights,
)


# WARNING: field name changes are not backward-compatible
@dataclass
@whiteboard(name='classification')
class Whiteboard:
    task_spec: TaskSpec
    lang: Optional[str]
    input_objects: List[Objects]
    control_objects: List[TaskSingleSolution]
    params: Params

    project: TolokaProject

    pool_id: str

    pool: TolokaPool  # with final attrs after closing
    assignments: List[TolokaAssignment]

    raw_results: Results
    worker_weights: Optional[WorkerWeights]


@op(lazy_arguments=False, cache=True, version='1.0')
def create_pool(
    loop: classification_loop.ClassificationLoop,
    control_objects: List[mapping.TaskSingleSolution],
    pool_cfg: pool_config.ClassificationConfig,
) -> str:
    return loop.create_pool(control_objects, pool_cfg).id


@op(lazy_arguments=False, cache=True, version='1.0')
def add_input_objects(
    loop: classification_loop.ClassificationLoop,
    input_objects: List[mapping.Objects],
    pool_id: str,
) -> None:
    # here and below we cast pool_id to str because lzy send value to @op in some wrapper which not JSON serializable
    # for toloka-kit requests
    loop.add_input_objects(str(pool_id), input_objects)


@op(lazy_arguments=False, cache=True, version='1.0')
def run_loop(
    loop: classification_loop.ClassificationLoop,
    pool_id: str,
) -> None:
    loop.loop(str(pool_id))


@op(lazy_arguments=False, cache=True, version='1.0')
def get_results(
    loop: classification_loop.ClassificationLoop,
    input_objects: List[mapping.Objects],
    pool_id: str,
) -> Tuple[classification.Results, Optional[classification.WorkerWeights]]:
    return loop.get_results(str(pool_id), input_objects)

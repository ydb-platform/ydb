import logging
from typing import List, Union, Optional

import toloka.client as toloka

from .. import experts, mapping, pricing, objects, task_spec as spec
from ..params import Params
from .task import check_project

logger = logging.getLogger(__name__)


def get_training_name(task_spec: spec.PreparedTaskSpec) -> str:
    return f'Training for {task_spec.log_message()}'


def _log_no_trainings(project_id: str, task_spec: spec.PreparedTaskSpec):
    logger.debug(f'No available trainings found for {task_spec.log_message()} in Toloka project {project_id}')


def _outdated_training_msg(training_id: str, task_spec: spec.PreparedTaskSpec, project_id: str) -> str:
    return f'Found outdated training {training_id} for {task_spec.log_message()} in Toloka project {project_id}'


# TODO (DATAFORGE-58): reliable work with training using lzy
def create_training(
    task_spec: spec.PreparedTaskSpec,
    solutions: List[Union[mapping.TaskSingleSolution, mapping.TaskMultipleSolutionOptions]],
    comments: List[objects.Text],
    client: toloka.TolokaClient,
    training_config: pricing.TrainingConfig,
    append_to_existing: bool = False,
):
    prj = check_project(task_spec, client)

    found_training = experts.find_training(prj.id, client)
    is_ok = found_training is not None and experts.training_is_suitable(
        found_training, training_config.get_task_duration_hint()
    )

    if found_training is None or not append_to_existing:
        _log_no_trainings(prj.id, task_spec)
        training = experts.create_training(prj.id, get_training_name(task_spec), training_config, client)
        logger.debug(f'Training {training.id} created')
    elif is_ok:
        training = found_training
        logger.debug(f'Reusing suitable training {training.id}')
    else:
        existing_training = found_training
        logger.debug(
            f'{_outdated_training_msg(existing_training.id, task_spec, prj.id)}, creating a new one with task reusing'
        )
        training = experts.create_training(
            prj.id, get_training_name(task_spec), training_config, client, reuse_tasks_from=existing_training
        )
        logger.debug(f'Training {training.id} created, reused tasks from {existing_training.id}')

    experts.add_training_tasks(training.id, task_spec.task_mapping, solutions, comments, client)
    client.open_training(training.id)


# TODO (DATAFORGE-58): reliable work with training using lzy
def find_training_requirement(
    project_id: str,
    task_spec: spec.PreparedTaskSpec,
    client: toloka.TolokaClient,
    params: Params,
) -> Optional[toloka.pool.QualityControl.TrainingRequirement]:
    if params.worker_filter.training_score is None:
        return None
    found_training = experts.find_training(project_id, client)
    if found_training is None:
        _log_no_trainings(project_id, task_spec)
        return None
    if not experts.training_is_suitable(found_training, params.task_duration_hint):
        outdated_training_msg = _outdated_training_msg(found_training.id, task_spec, project_id)
        available_task_count = len(list(client.get_tasks(pool_id=found_training.id)))
        try:
            training_config = pricing.TrainingConfig.from_toloka_training(
                found_training, params.task_duration_hint, available_task_count
            )
        except AssertionError as e:
            # task duration hint changes significantly, no way to correctly recreate training
            logger.warn(
                f'{outdated_training_msg}. Failed to recreate training due to a change in task duration hint, '
                f'using outdated version. Consider the following problem: "{e}".'
            )
        else:
            logger.debug(f'{outdated_training_msg}. Recreating with task reusing.')
            found_training = experts.create_training(
                project_id, get_training_name(task_spec), training_config, client, reuse_tasks_from=found_training
            )
            client.open_training(found_training.id)

    logger.debug(f'Using training {found_training.id}')

    return toloka.pool.QualityControl.TrainingRequirement(
        training_pool_id=found_training.id,
        training_passing_skill_value=params.worker_filter.training_score,
    )

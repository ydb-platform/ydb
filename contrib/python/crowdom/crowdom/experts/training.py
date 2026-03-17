from datetime import timedelta
import logging
from typing import List, Optional, Union

import toloka.client as toloka

from .. import mapping, objects, pricing

logger = logging.getLogger(__name__)

min_duration_ratio = 0.4
max_duration_ratio = 2.5


def training_is_suitable(training: toloka.Training, task_duration_hint: timedelta) -> bool:
    if training.status != toloka.Training.Status.OPEN:
        # Toloka sometimes closes trainings on its own, this may be due to too many completed assignments.
        # In this case, we need to create the training again, regardless of whether it is suitable or not.
        return False
    task_duration_hint_with_overhead = task_duration_hint * pricing.training_overhead_multiplier
    existing_task_duration_hint_with_overhead = timedelta(
        seconds=(training.assignment_max_duration_seconds / training.training_tasks_in_task_suite_count)
    )
    return (
        min_duration_ratio
        <= existing_task_duration_hint_with_overhead / task_duration_hint_with_overhead
        <= max_duration_ratio
    )


def find_training(
    project_id: str,
    client: toloka.TolokaClient,
) -> Optional[toloka.Training]:
    # Toloka API doesn't support multiple training statuses in search for some reason. So we make two calls.
    found_trainings = []
    for status in (toloka.Training.Status.OPEN, toloka.Training.Status.CLOSED):
        found_trainings += client.find_trainings(
            project_id=project_id, status=status, sort=toloka.search_requests.TrainingSortItems(['-created']), limit=1
        ).items
    if not found_trainings:
        return None
    return sorted(found_trainings, key=lambda t: t.created, reverse=True)[0]  # current training


def create_training(
    project_id: str,
    private_name: str,
    training_config: pricing.TrainingConfig,
    client: toloka.TolokaClient,
    reuse_tasks_from: Optional[toloka.Training] = None,
) -> toloka.Training:
    training = toloka.training.Training(
        project_id=project_id,
        private_name=private_name,
        may_contain_adult_content=True,
        assignment_max_duration_seconds=int(training_config.max_assignment_duration.total_seconds()),
        mix_tasks_in_creation_order=True,
        shuffle_tasks_in_task_suite=True,
        training_tasks_in_task_suite_count=training_config.training_tasks_in_task_suite_count,
        task_suites_required_to_pass=training_config.task_suites_required_to_pass,
        retry_training_after_days=30,  # todo
        inherited_instructions=True,
    )
    created_training = client.create_training(training)

    if reuse_tasks_from is not None:
        existing_tasks = list(client.get_tasks(pool_id=reuse_tasks_from.id))
        for task in existing_tasks:
            task.pool_id = created_training.id
        client.create_tasks(existing_tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)

    return created_training


def _get_training_task(task_mapping: mapping.TaskMapping, solution: mapping.Objects) -> toloka.Task.KnownSolution:
    return toloka.Task.KnownSolution(output_values=task_mapping.toloka_values(solution, output=True))


def _get_training_tasks(
    task_mapping: mapping.TaskMapping, solution: Union[mapping.Objects, List[mapping.Objects]]
) -> List[toloka.Task.KnownSolution]:
    if isinstance(solution, list):
        return [_get_training_task(task_mapping, single_solution) for single_solution in solution]
    return [_get_training_task(task_mapping, solution)]


def add_training_tasks(
    training_id: str,
    task_mapping: mapping.TaskMapping,
    solutions: List[Union[mapping.TaskSingleSolution, mapping.TaskMultipleSolutionOptions]],
    comments: List[objects.Text],
    client: toloka.TolokaClient,
):
    training_tasks = [
        toloka.Task(
            pool_id=training_id,
            input_values=task_mapping.toloka_values(task),
            known_solutions=_get_training_tasks(task_mapping, solution),
            message_on_unknown_solution=comment.text,
        )
        for (task, solution), comment in zip(solutions, comments)
    ]
    logger.debug(f'Adding {len(training_tasks)} training tasks to training {training_id}')
    client.create_tasks(training_tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)


def get_skill_id_from_training(training: toloka.Pool) -> str:
    for confing in training.quality_control.configs:
        for rule in confing.rules:
            if isinstance(rule.action, toloka.actions.SetSkillFromOutputField):
                return rule.action.parameters.skill_id
    assert False, 'unreachable'

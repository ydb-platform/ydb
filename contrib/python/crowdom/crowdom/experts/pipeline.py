from dataclasses import dataclass
import logging
from typing import List, Union, Tuple

import toloka.client as toloka

from .. import base, classification_loop, mapping, project as project_config, pool as pool_config, utils


logger = logging.getLogger(__name__)


# scenario automatically changes TaskMapping for PreparedTaskSpec (see builders.py)
# - EXPERT_LABELING_OF_TASKS adds expert meta fields (_ok, _comment) to outputs
# - EXPERT_LABELING_OF_SOLVED_TASKS moves function outputs to inputs, and expert meta fields is outputs
#
# classification function
#     **expert case**     **required fields**                 **arguments**
#     task markup:        inputs | outputs   _ok _comment     [scenario == EXPERT_LABELING_OF_TASKS]
#     solution markup:    inputs   outputs | _ok _comment     [scenario == EXPERT_LABELING_OF_SOLVED_TASKS]
#     verification:       inputs   outputs | _ok _comment     [scenario == EXPERT_LABELING_OF_SOLVED_TASKS]
#
# for annotation function, expected evaluation of annotation is added to expert meta fields, resulting to (eval, _ok, _comment),
# for all expert markup cases except verification case (see AnnotationBuilder in builders.py)
# - for solution markup we use same scenario EXPERT_LABELING_OF_TASKS as for task markup, but we use nested task_spec.check
# - for verification, we do not need eval column, but we need to move outputs to inputs so we use EXPERT_LABELING_OF_SOLVED_TASKS
#
# annotation function
#     **expert case**     **required fields**                     **arguments**
#     task markup:        inputs | outputs   eval _ok _comment    [scenario == EXPERT_LABELING_OF_TASKS]
#     solution markup:    inputs   outputs | eval _ok _comment    [scenario == EXPERT_LABELING_OF_TASKS, task_spec == task_spec.check]
#     verification:       inputs   outputs |      _ok _comment    [scenario == EXPERT_LABELING_OF_SOLVED_TASKS]
#
#     eval column is added to expert meta by AnnotationBuilder
@dataclass
class ExpertPipeline:
    client: toloka.TolokaClient
    task_function: base.TaskFunction
    task_mapping: mapping.TaskMapping
    lang: str
    scenario: project_config.Scenario

    def __post_init__(self):
        task_annotation = self.scenario == project_config.Scenario.EXPERT_LABELING_OF_TASKS
        solution_annotation = self.scenario == project_config.Scenario.EXPERT_LABELING_OF_SOLVED_TASKS
        annotation_verification = False
        if isinstance(self.task_function, base.AnnotationFunction):
            # for task labeling, we have task function outputs,
            # expert annotation fields and extra evaluation field in output mapping
            task_annotation = (
                len(self.task_mapping.output_mapping)
                == len(self.task_function.outputs) + len(project_config.Builder.EXPERT_LABELING_METAS) + 1
            )

            # for solution annotation, we only have expert annotation fields and evaluation in output mapping
            solution_annotation = (
                len(self.task_mapping.output_mapping) == len(project_config.Builder.EXPERT_LABELING_METAS) + 1
            )

            annotation_verification = len(self.task_mapping.output_mapping) == len(
                project_config.Builder.EXPERT_LABELING_METAS
            )

        assert annotation_verification or task_annotation or solution_annotation, 'Incorrect configuration'
        self.scenario = (
            project_config.Scenario.EXPERT_LABELING_OF_TASKS
            if task_annotation
            else project_config.Scenario.EXPERT_LABELING_OF_SOLVED_TASKS
        )

    def create_pool(
        self,
        pool_cfg: pool_config.ExpertConfig,
    ) -> toloka.Pool:
        pool = self.client.create_pool(pool_config.create_expert_pool_params(pool_cfg))
        return pool

    def add_input_objects(
        self, pool_id: str, input_objects: Union[List[mapping.TaskSingleSolution], List[mapping.Objects]]
    ):
        assert input_objects, 'No objects supplied'
        # todo not very reliable typecheck due to dynamic generics
        # todo: verification case for annotations

        solution_annotation = (
            isinstance(input_objects[0], tuple)
            and len(input_objects[0]) == 2
            and isinstance(input_objects[0][0], tuple)
            and isinstance(input_objects[0][1], tuple)
        )
        tasks = []
        for task_objects_or_solutions in input_objects:
            if solution_annotation:
                task_objects_or_solutions = task_objects_or_solutions[0] + task_objects_or_solutions[1]
            self.task_mapping.validate_objects(task_objects_or_solutions)
            task = self.task_mapping.to_task(task_objects_or_solutions)
            task.pool_id = pool_id
            tasks.append(task)
        logger.debug(f'creating {len(tasks)} tasks')
        self.client.create_tasks(tasks, allow_defaults=True, async_mode=True, skip_invalid_items=False)
        self.client.open_pool(pool_id)

    def loop(self, pool_id: str):
        utils.wait_pool_for_close(self.client, pool_id)

    def get_results(
        self,
        pool_id: str,
        objects: Union[List[mapping.TaskSingleSolution], List[mapping.Objects]],
    ) -> List[Tuple[Union[mapping.TaskSingleSolution, mapping.Objects], mapping.TaskMultipleSolutions]]:
        assignments = [
            assignment
            for assignment, _ in classification_loop.get_assignments_solutions(
                self.client, self.task_mapping, pool_id, [toloka.Assignment.ACCEPTED]
            )
        ]
        if self.scenario == project_config.Scenario.EXPERT_LABELING_OF_SOLVED_TASKS:
            objects = [obj[0] + obj[1] for obj in objects]
        return list(zip(objects, mapping.get_solutions(assignments, self.task_mapping, objects)))

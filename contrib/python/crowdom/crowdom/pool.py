import abc
from dataclasses import dataclass, field
import datetime
from typing import Optional

import toloka.client as toloka

from . import control, worker


class PoolBuilder:
    def __init__(
        self,
        project_id: str,
        private_name: str,
        public_description: Optional[str],
        reward_per_assignment: float,
        assignment_duration_hint: datetime.timedelta,
        real_tasks_count: int,
        control_tasks_count: int,
        ttl_days: int = 30,
        auto_accept_period_day: int = 14,
        priority: int = 30,
        may_contain_adult_content=True,
    ):
        pool = toloka.Pool(
            project_id=project_id,
            private_name=private_name,
            public_description=public_description,
            may_contain_adult_content=may_contain_adult_content,
            will_expire=datetime.datetime.utcnow() + datetime.timedelta(days=ttl_days),
            reward_per_assignment=reward_per_assignment,
            assignment_max_duration_seconds=int((assignment_duration_hint * 5).total_seconds()),
            auto_accept_solutions=False,
            auto_accept_period_day=auto_accept_period_day,
            assignments_issuing_config=toloka.Pool.AssignmentsIssuingConfig(issue_task_suites_in_creation_order=False),
            priority=priority,
        )

        pool.set_mixer_config(
            real_tasks_count=real_tasks_count, golden_tasks_count=control_tasks_count, training_tasks_count=0
        )

        self.pool = pool
        self.real_tasks_count = real_tasks_count
        self.control_tasks_count = control_tasks_count
        self.tasks_in_assignment = real_tasks_count + control_tasks_count
        self.may_contain_adult_content = may_contain_adult_content
        self.assignment_duration_hint = assignment_duration_hint

    def build(self) -> toloka.Pool:
        return self.pool

    def set_overlap(self, overlap: int) -> 'PoolBuilder':
        self.pool.set_defaults(
            defaults=toloka.Pool.Defaults(
                default_overlap_for_new_tasks=overlap, default_overlap_for_new_task_suites=overlap
            )
        )
        return self

    def set_worker_filter(self, human_filter: Optional[worker.HumanFilter]) -> 'PoolBuilder':
        if human_filter is None:
            return self
        assert self.pool.filter is None, 'repeated set of worker filter'
        self.pool.filter = human_filter.to_toloka_filter()
        return self

    def add_skipped_assignments(
        self,
        skipped_count: int = 5,
        duration: int = 8,
        duration_unit: toloka.user_restriction.DurationUnit = toloka.user_restriction.DurationUnit.HOURS,
        scope: toloka.UserRestriction = toloka.UserRestriction.PROJECT,
    ) -> 'PoolBuilder':
        self.pool.quality_control.add_action(
            collector=toloka.collectors.SkippedInRowAssignments(),
            conditions=[toloka.conditions.SkippedInRowCount >= skipped_count],
            action=toloka.actions.RestrictionV2(
                private_comment='Skipped assignments', duration=duration, duration_unit=duration_unit, scope=scope
            ),
        )
        return self

    def add_vacation(
        self,
        work_duration_before_vacation: datetime.timedelta,
        duration: int = 5,
        duration_unit: toloka.user_restriction.DurationUnit = toloka.user_restriction.DurationUnit.HOURS,
        scope: toloka.UserRestriction = toloka.UserRestriction.PROJECT,
    ) -> 'PoolBuilder':
        # 1h of tasks approx
        accepted_count = int(work_duration_before_vacation / self.assignment_duration_hint)
        assert accepted_count > 0
        self.pool.quality_control.add_action(
            collector=toloka.collectors.AnswerCount(),
            conditions=[toloka.conditions.AssignmentsAcceptedCount >= accepted_count],
            action=toloka.actions.RestrictionV2(
                private_comment='Completed many assignments, vacation',
                duration=duration,
                duration_unit=duration_unit,
                scope=scope,
            ),
        )
        return self

    def add_training(
        self,
        training_requirement: Optional[toloka.pool.QualityControl.TrainingRequirement],
    ) -> 'PoolBuilder':
        if training_requirement is not None:
            self.pool.set_training_requirement(training_requirement)
        return self

    def add_control_rules(self, control_params: Optional[control.Control]) -> 'PoolBuilder':
        if control_params is not None:
            toloka_rules = control_params.to_toloka_quality_control(
                self.control_tasks_count, self.assignment_duration_hint
            )
            for toloka_rule in toloka_rules:
                self.pool.quality_control.add_action(**toloka_rule)
        return self

    def set_auto_accept(self) -> 'PoolBuilder':
        self.pool.auto_accept_solutions = True
        return self

    def add_quality_adjustment(self, percent: Optional[int] = 90):
        if percent:
            self.pool.set_speed_quality_balance(
                toloka.pool.speed_quality_balance_config.TopPercentageByQuality(percent=percent)
            )


@dataclass
class BaseConfig:
    project_id: str
    private_name: str
    reward_per_assignment: float
    task_duration_hint: datetime.timedelta
    real_tasks_count: int
    priority: int = 30
    ttl_days: int = 30
    may_contain_adult_content: bool = True
    public_description: Optional[str] = None

    @abc.abstractmethod
    def get_tasks_count(self) -> int:
        ...


@dataclass
class CrowdConfig(BaseConfig, abc.ABC):
    worker_filter: Optional[worker.BaseWorkerFilter] = None
    training_requirement: Optional[toloka.pool.QualityControl.TrainingRequirement] = None
    auto_accept_period_day: int = 14
    work_duration_before_vacation: datetime.timedelta = datetime.timedelta(hours=1)
    control_params: Optional[control.Control] = None
    adjust_percent: Optional[int] = 90


@dataclass
class MarkupConfig(CrowdConfig):
    # TODO(DATAFORGE-75): works only for current markup model worker limitations
    first_attempt_by_model_worker: bool = False

    def get_tasks_count(self) -> int:
        return self.real_tasks_count


@dataclass
class ClassificationConfig(CrowdConfig):
    control_tasks_count: int = 3
    overlap: int = 3

    def get_tasks_count(self) -> int:
        return self.real_tasks_count + self.control_tasks_count


@dataclass
class ExpertConfig(BaseConfig):
    worker_filter: worker.ExpertFilter = field(default_factory=lambda: worker.ExpertFilter(skills=[]))

    def get_tasks_count(self) -> int:
        return self.real_tasks_count


def create_pool_params(config: CrowdConfig):
    assert isinstance(config, MarkupConfig) or isinstance(config, ClassificationConfig)

    if isinstance(config, MarkupConfig):
        control_tasks_count = 0
        overlap = 0 if config.first_attempt_by_model_worker else 1
    else:
        control_tasks_count = config.control_tasks_count
        overlap = config.overlap

    task_count = config.get_tasks_count()

    assignment_duration_hint = config.task_duration_hint * task_count

    builder = PoolBuilder(
        project_id=config.project_id,
        private_name=config.private_name,
        public_description=config.public_description,
        reward_per_assignment=config.reward_per_assignment,
        assignment_duration_hint=assignment_duration_hint,
        real_tasks_count=config.real_tasks_count,
        control_tasks_count=control_tasks_count,
        priority=config.priority,
        auto_accept_period_day=config.auto_accept_period_day,
        may_contain_adult_content=config.may_contain_adult_content,
        ttl_days=config.ttl_days,
    )

    builder.set_worker_filter(config.worker_filter).add_training(
        config.training_requirement
    ).add_skipped_assignments().add_control_rules(config.control_params).add_vacation(
        work_duration_before_vacation=config.work_duration_before_vacation
    ).set_overlap(
        overlap
    ).add_quality_adjustment(
        config.adjust_percent
    )

    return builder.build()


def create_expert_pool_params(config: ExpertConfig):
    control_tasks_count, overlap = 0, 1

    task_count = config.get_tasks_count()

    assignment_duration_hint = config.task_duration_hint * task_count

    builder = PoolBuilder(
        project_id=config.project_id,
        private_name=config.private_name,
        public_description=config.public_description,
        reward_per_assignment=config.reward_per_assignment,
        assignment_duration_hint=assignment_duration_hint,
        real_tasks_count=config.real_tasks_count,
        control_tasks_count=control_tasks_count,
        priority=config.priority,
        may_contain_adult_content=config.may_contain_adult_content,
        ttl_days=config.ttl_days,
    )

    builder.set_worker_filter(config.worker_filter).set_overlap(overlap).set_auto_accept()

    return builder.build()

__all__ = [
    'dynamic_overlap_config',
    'dynamic_pricing_config',
    'mixer_config',

    'Pool',
    'PoolPatchRequest',
    'DynamicOverlapConfig',
    'DynamicPricingConfig',
    'MixerConfig',
    'SpeedQualityBalanceConfig',
]
import datetime
from enum import unique
from typing import Dict, List, Optional

import attr

from . import dynamic_overlap_config
from . import dynamic_pricing_config
from . import mixer_config

from .dynamic_overlap_config import DynamicOverlapConfig
from .dynamic_pricing_config import DynamicPricingConfig
from .mixer_config import MixerConfig
from .speed_quality_balance_config import SpeedQualityBalanceConfig
from .._converter import unstructure
from ..filter import FilterCondition, FilterOr, FilterAnd, Condition
from ..owner import Owner
from ..primitives.base import BaseTolokaObject
from ..quality_control import QualityControl
from ...util._codegen import attribute, codegen_attr_attributes_setters, create_setter, expand
from ...util._extendable_enum import ExtendableStrEnum


@codegen_attr_attributes_setters
class Pool(BaseTolokaObject):
    """A set of [tasks](toloka.client.task.Task.md) that share the same properties.

    In the pool properties, you set the task price, overlap, Toloker selection filters, quality control rules, and so on.

    Pool tasks are grouped into [task suites](toloka.client.task_suite.TaskSuite.md). Whole task suites are assigned to Tolokers.

    Learn more about:
    * [Pools](https://toloka.ai/docs/guide/pool-main)
    * [Pricing](https://toloka.ai/docs/guide/dynamic-pricing)

    Attributes:
        project_id: The ID of the project containing the pool.
        private_name: The pool name. It is visible to the requester and is not visible to Tolokers.
        may_contain_adult_content: The presence of adult content.
        reward_per_assignment: Payment in US dollars for a Toloker for completing a task suite. For cents, use the dot as a separator.
            If the pool `type` is `REGULAR`, the minimum payment per task suite is $0.005. For other pool types, you can set the `reward_per_assignment` to zero.
        assignment_max_duration_seconds: Time limit to complete one task suite.
            Take into account loading a page with a task suite and sending responses to the server. It is recommended that you set at least 60 seconds.
            Tasks not completed within the limit are reassigned to other Tolokers.
        defaults: Default settings that are applied to new tasks in the pool.
        will_expire: The UTC date and time when the pool is closed automatically, even if not all tasks are completed.
        private_comment: A comment about the pool. It is visible to the requester and is not visible to Tolokers.
        public_description: The pool description. If pool's `public_description` is not set, then project's `public_description` is used.
        public_instructions: The pool instructions for Tolokers. If pool's `public_instructions` is not set, then project's `public_instructions` is used.
        auto_close_after_complete_delay_seconds: The pool remains open after all tasks are completed during the specified time in seconds.

            Use non zero value if:
            * You process data in real time.
            * The pool must stay open so that you can upload new tasks.
            * Dynamic overlap is enabled in the pool.

            Allowed range: from 0 to 259200 seconds (3 days). The default value is 0.
        dynamic_pricing_config: The dynamic pricing settings.
        auto_accept_solutions:
            * `True` — Responses from Tolokers are accepted or rejected automatically based on some rules.
            * `False` — Responses are checked manually. Time reserved for checking is limited by the `auto_accept_period_day` parameter.
                Learn more about [non-automatic acceptance](https://toloka.ai/docs/guide/offline-accept).
        auto_accept_period_day: The number of days reserved for checking responses if the `auto_accept_solutions` parameter is set to `False`.
        assignments_issuing_config: Settings for assigning tasks in the pool.
        priority: The priority of the pool in relation to other pools in the project with the same task price and set of filters.
            Tolokers are assigned tasks with a higher priority first.

            Allowed range: from 0 to 100. The default value is 0.
        filter: Settings for Toloker selection filters.
        quality_control: Settings for quality control rules and the ID of the pool with training tasks.
        speed_quality_balance: Settings for choosing Tolokers for your tasks.
        dynamic_overlap_config: Dynamic overlap settings.
        mixer_config: Parameters for automatically creating task suites.
        training_config: Additional settings for linked training.
        metadata: A dictionary with metadata.
        owner: The pool owner.
        id: The ID of the pool. Read-only field.
        status: The status of the pool. Read-only field.
        last_close_reason: A reason why the pool was closed last time. Read-only field.
        created: The UTC date and time when the pool was created. Read-only field.
        last_started: The UTC date and time when the pool was started last time. Read-only field.
        last_stopped: The UTC date and time when the pool was stopped last time. Read-only field.
        type: The type of the pool. Deprecated.

    Example:
        Creating a new pool.

        >>> new_pool = toloka.client.Pool(
        >>>     project_id='92694',
        >>>     private_name='Experimental pool',
        >>>     may_contain_adult_content=False,
        >>>     will_expire=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365),
        >>>     reward_per_assignment=0.01,
        >>>     assignment_max_duration_seconds=60*20,
        >>>     defaults=toloka.client.Pool.Defaults(default_overlap_for_new_task_suites=3),
        >>>     filter=toloka.client.filter.Languages.in_('EN'),
        >>> )
        >>> new_pool.set_mixer_config(real_tasks_count=10)
        >>> new_pool = toloka_client.create_pool(new_pool)
        >>> print(new_pool.id)
        ...
    """

    class AssignmentsIssuingConfig(BaseTolokaObject, kw_only=False):
        """Settings for assigning task suites in the pool.

        Attributes:
            issue_task_suites_in_creation_order:
                Task suites are assigned in the order in which they were created.

                This parameter is used when tasks are [grouped into suites](https://toloka.ai/docs/guide/distribute-tasks-by-pages)
                manually and the `assignments_issuing_type` project parameter is set to `AUTOMATED`.
        """

        issue_task_suites_in_creation_order: bool

    @unique
    class CloseReason(ExtendableStrEnum):
        """The reason for closing the pool.

        Attributes:
            MANUAL: A pool was closed by a requester.
            EXPIRED: The lifetime of the pool expired.
            COMPLETED: All tasks were completed.
            NOT_ENOUGH_BALANCE: There is not enough money to run the pool.
            ASSIGNMENTS_LIMIT_EXCEEDED: A limit of 2 million assignments is reached.
            BLOCKED: The requester's account was blocked.
            FOR_UPDATE: Pool parameters are changing at the moment.
        """

        MANUAL = 'MANUAL'
        EXPIRED = 'EXPIRED'
        COMPLETED = 'COMPLETED'
        NOT_ENOUGH_BALANCE = 'NOT_ENOUGH_BALANCE'
        ASSIGNMENTS_LIMIT_EXCEEDED = 'ASSIGNMENTS_LIMIT_EXCEEDED'
        BLOCKED = 'BLOCKED'
        FOR_UPDATE = 'FOR_UPDATE'

    class Defaults(BaseTolokaObject):
        """Default settings that are applied to new tasks and task suites in a pool.

           These settings are used when tasks or task suites are created with `allow_defaults=True`.

        Attributes:
            default_overlap_for_new_task_suites: The default overlap of a task suite.
            default_overlap_for_new_tasks: The default overlap of a task.
        """

        default_overlap_for_new_task_suites: int
        default_overlap_for_new_tasks: int

    @unique
    class Status(ExtendableStrEnum):
        """The status of a pool.

        Attributes:
            OPEN: The pool is open.
            CLOSED: The pool is closed.
            ARCHIVED: The pool is archived.
            LOCKED: The pool is locked.
        """

        OPEN = 'OPEN'
        CLOSED = 'CLOSED'
        ARCHIVED = 'ARCHIVED'
        LOCKED = 'LOCKED'

    class TrainingConfig(BaseTolokaObject, kw_only=False):
        training_skill_ttl_days: int

    @unique
    class Type(ExtendableStrEnum):
        """The type of a pool.
        """

        REGULAR = 'REGULAR'
        TRAINING = 'TRAINING'

    DynamicOverlapConfig = DynamicOverlapConfig
    DynamicPricingConfig = DynamicPricingConfig
    MixerConfig = MixerConfig
    QualityControl = QualityControl

    project_id: str
    private_name: str
    may_contain_adult_content: bool
    reward_per_assignment: float
    assignment_max_duration_seconds: int
    defaults: Defaults = attr.attrib(factory=lambda: Pool.Defaults(default_overlap_for_new_task_suites=1))

    will_expire: datetime.datetime

    private_comment: str
    public_description: str
    public_instructions: str
    auto_close_after_complete_delay_seconds: int
    dynamic_pricing_config: DynamicPricingConfig

    auto_accept_solutions: bool
    auto_accept_period_day: int
    assignments_issuing_config: AssignmentsIssuingConfig
    priority: int
    filter: FilterCondition
    quality_control: QualityControl = attr.attrib(factory=QualityControl)
    speed_quality_balance: SpeedQualityBalanceConfig
    dynamic_overlap_config: DynamicOverlapConfig
    mixer_config: MixerConfig
    training_config: TrainingConfig

    metadata: Dict[str, List[str]]
    owner: Owner

    # Readonly
    id: str = attribute(readonly=True)
    status: Status = attribute(readonly=True)
    last_close_reason: CloseReason = attribute(readonly=True)
    created: datetime.datetime = attribute(readonly=True)
    last_started: datetime.datetime = attribute(readonly=True)
    last_stopped: datetime.datetime = attribute(readonly=True)
    type: Type = attribute(readonly=True)

    def unstructure(self) -> Optional[dict]:
        self_unstructured_dict = super().unstructure()
        if isinstance(self.filter, Condition):
            self_unstructured_dict['filter'] = unstructure(FilterAnd([FilterOr([self.filter])]))
        if isinstance(self.filter, FilterOr):
            self_unstructured_dict['filter'] = unstructure(FilterAnd([self.filter]))
        if isinstance(self.filter, FilterAnd):
            self_unstructured_dict['filter'] = unstructure(FilterAnd([
                unstructure(FilterOr([inner_filter]))
                if isinstance(inner_filter, Condition)
                else unstructure(inner_filter)
                for inner_filter in self.filter
            ]))
        return self_unstructured_dict

    def is_open(self) -> bool:
        return self.status == Pool.Status.OPEN

    def is_closed(self) -> bool:
        return self.status == Pool.Status.CLOSED

    def is_archived(self) -> bool:
        return self.status == Pool.Status.ARCHIVED

    def is_locked(self) -> bool:
        return self.status == Pool.Status.LOCKED

    set_training_requirement = expand('training_requirement')(create_setter(
        'quality_control.training_requirement',
        QualityControl.TrainingRequirement,
        __name__,
    ))

    set_captcha_frequency = expand('captcha_frequency')(create_setter(
        'quality_control.captcha_frequency',
        QualityControl.CaptchaFrequency,
        __name__,
    ))

    set_checkpoints_config = expand('checkpoints_config')(create_setter(
        'quality_control.checkpoints_config',
        QualityControl.CheckpointsConfig,
        __name__,
    ))

    set_quality_control_configs = create_setter('quality_control.configs', module=__name__)
    set_quality_control_configs.__doc__ = """A shortcut method for setting """


class PoolPatchRequest(BaseTolokaObject, kw_only=False):
    """New pool parameters.

    This class is used with the [patch_pool](toloka.client.TolokaClient.patch_pool.md) method.

    Attributes:
        priority: The new priority of the pool.
            Possible values: from -100 to 100.
    """

    priority: int

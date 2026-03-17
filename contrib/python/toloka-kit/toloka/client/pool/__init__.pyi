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
import toloka.client.filter
import toloka.client.owner
import toloka.client.pool.dynamic_overlap_config
import toloka.client.pool.dynamic_pricing_config
import toloka.client.pool.mixer_config
import toloka.client.pool.speed_quality_balance_config
import toloka.client.primitives.base
import toloka.client.quality_control
import toloka.client.task_distribution_function
import toloka.util._extendable_enum
import typing

from toloka.client.pool import (
    dynamic_overlap_config,
    dynamic_pricing_config,
    mixer_config,
)
from toloka.client.pool.dynamic_overlap_config import DynamicOverlapConfig
from toloka.client.pool.dynamic_pricing_config import DynamicPricingConfig
from toloka.client.pool.mixer_config import MixerConfig
from toloka.client.pool.speed_quality_balance_config import SpeedQualityBalanceConfig


class Pool(toloka.client.primitives.base.BaseTolokaObject):
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

    class AssignmentsIssuingConfig(toloka.client.primitives.base.BaseTolokaObject):
        """Settings for assigning task suites in the pool.

        Attributes:
            issue_task_suites_in_creation_order:
                Task suites are assigned in the order in which they were created.

                This parameter is used when tasks are [grouped into suites](https://toloka.ai/docs/guide/distribute-tasks-by-pages)
                manually and the `assignments_issuing_type` project parameter is set to `AUTOMATED`.
        """

        def __init__(self, issue_task_suites_in_creation_order: typing.Optional[bool] = None) -> None:
            """Method generated by attrs for class Pool.AssignmentsIssuingConfig.
            """
            ...

        _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
        issue_task_suites_in_creation_order: typing.Optional[bool]

    class CloseReason(toloka.util._extendable_enum.ExtendableStrEnum):
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

    class Defaults(toloka.client.primitives.base.BaseTolokaObject):
        """Default settings that are applied to new tasks and task suites in a pool.

           These settings are used when tasks or task suites are created with `allow_defaults=True`.

        Attributes:
            default_overlap_for_new_task_suites: The default overlap of a task suite.
            default_overlap_for_new_tasks: The default overlap of a task.
        """

        def __init__(
            self,
            *,
            default_overlap_for_new_task_suites: typing.Optional[int] = None,
            default_overlap_for_new_tasks: typing.Optional[int] = None
        ) -> None:
            """Method generated by attrs for class Pool.Defaults.
            """
            ...

        _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
        default_overlap_for_new_task_suites: typing.Optional[int]
        default_overlap_for_new_tasks: typing.Optional[int]

    class Status(toloka.util._extendable_enum.ExtendableStrEnum):
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

    class TrainingConfig(toloka.client.primitives.base.BaseTolokaObject):
        def __init__(self, training_skill_ttl_days: typing.Optional[int] = None) -> None:
            """Method generated by attrs for class Pool.TrainingConfig.
            """
            ...

        _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
        training_skill_ttl_days: typing.Optional[int]

    class Type(toloka.util._extendable_enum.ExtendableStrEnum):
        """The type of a pool.
        """

        REGULAR = 'REGULAR'
        TRAINING = 'TRAINING'

    def unstructure(self) -> typing.Optional[dict]: ...

    def is_open(self) -> bool: ...

    def is_closed(self) -> bool: ...

    def is_archived(self) -> bool: ...

    def is_locked(self) -> bool: ...

    @typing.overload
    def set_training_requirement(self, training_requirement: toloka.client.quality_control.QualityControl.TrainingRequirement):
        """A shortcut setter for quality_control.training_requirement
        """
        ...

    @typing.overload
    def set_training_requirement(
        self,
        *,
        training_pool_id: typing.Optional[str] = None,
        training_passing_skill_value: typing.Optional[int] = None
    ):
        """A shortcut setter for quality_control.training_requirement
        """
        ...

    @typing.overload
    def set_captcha_frequency(self, captcha_frequency: toloka.client.quality_control.QualityControl.CaptchaFrequency):
        """A shortcut setter for quality_control.captcha_frequency
        """
        ...

    @typing.overload
    def set_captcha_frequency(
        self,
        *args,
        **kwargs
    ):
        """A shortcut setter for quality_control.captcha_frequency
        """
        ...

    @typing.overload
    def set_checkpoints_config(self, checkpoints_config: toloka.client.quality_control.QualityControl.CheckpointsConfig):
        """A shortcut setter for quality_control.checkpoints_config
        """
        ...

    @typing.overload
    def set_checkpoints_config(
        self,
        *,
        real_settings: typing.Optional[toloka.client.quality_control.QualityControl.CheckpointsConfig.Settings] = None,
        golden_settings: typing.Optional[toloka.client.quality_control.QualityControl.CheckpointsConfig.Settings] = None,
        training_settings: typing.Optional[toloka.client.quality_control.QualityControl.CheckpointsConfig.Settings] = None
    ):
        """A shortcut setter for quality_control.checkpoints_config
        """
        ...

    def set_quality_control_configs(self, configs):
        """A shortcut method for setting
        """
        ...

    def __init__(
        self,
        *,
        project_id: typing.Optional[str] = None,
        private_name: typing.Optional[str] = None,
        may_contain_adult_content: typing.Optional[bool] = None,
        reward_per_assignment: typing.Optional[float] = None,
        assignment_max_duration_seconds: typing.Optional[int] = None,
        defaults: typing.Optional[Defaults] = ...,
        will_expire: typing.Optional[datetime.datetime] = None,
        private_comment: typing.Optional[str] = None,
        public_description: typing.Optional[str] = None,
        public_instructions: typing.Optional[str] = None,
        auto_close_after_complete_delay_seconds: typing.Optional[int] = None,
        dynamic_pricing_config: typing.Optional[toloka.client.pool.dynamic_pricing_config.DynamicPricingConfig] = None,
        auto_accept_solutions: typing.Optional[bool] = None,
        auto_accept_period_day: typing.Optional[int] = None,
        assignments_issuing_config: typing.Optional[AssignmentsIssuingConfig] = None,
        priority: typing.Optional[int] = None,
        filter: typing.Optional[toloka.client.filter.FilterCondition] = None,
        quality_control: typing.Optional[toloka.client.quality_control.QualityControl] = ...,
        speed_quality_balance: typing.Optional[toloka.client.pool.speed_quality_balance_config.SpeedQualityBalanceConfig] = None,
        dynamic_overlap_config: typing.Optional[toloka.client.pool.dynamic_overlap_config.DynamicOverlapConfig] = None,
        mixer_config: typing.Optional[toloka.client.pool.mixer_config.MixerConfig] = None,
        training_config: typing.Optional[TrainingConfig] = None,
        metadata: typing.Optional[typing.Dict[str, typing.List[str]]] = None,
        owner: typing.Optional[toloka.client.owner.Owner] = None,
        id: typing.Optional[str] = None,
        status: typing.Optional[Status] = None,
        last_close_reason: typing.Optional[CloseReason] = None,
        created: typing.Optional[datetime.datetime] = None,
        last_started: typing.Optional[datetime.datetime] = None,
        last_stopped: typing.Optional[datetime.datetime] = None,
        type: typing.Optional[Type] = None
    ) -> None:
        """Method generated by attrs for class Pool.
        """
        ...

    @typing.overload
    def set_defaults(self, defaults: Defaults):
        """A shortcut setter for defaults
        """
        ...

    @typing.overload
    def set_defaults(
        self,
        *,
        default_overlap_for_new_task_suites: typing.Optional[int] = None,
        default_overlap_for_new_tasks: typing.Optional[int] = None
    ):
        """A shortcut setter for defaults
        """
        ...

    @typing.overload
    def set_dynamic_pricing_config(self, dynamic_pricing_config: toloka.client.pool.dynamic_pricing_config.DynamicPricingConfig):
        """A shortcut setter for dynamic_pricing_config
        """
        ...

    @typing.overload
    def set_dynamic_pricing_config(
        self,
        type: typing.Union[toloka.client.pool.dynamic_pricing_config.DynamicPricingConfig.Type, str, None] = None,
        skill_id: typing.Optional[str] = None,
        intervals: typing.Optional[typing.List[toloka.client.pool.dynamic_pricing_config.DynamicPricingConfig.Interval]] = None
    ):
        """A shortcut setter for dynamic_pricing_config
        """
        ...

    @typing.overload
    def set_assignments_issuing_config(self, assignments_issuing_config: AssignmentsIssuingConfig):
        """A shortcut setter for assignments_issuing_config
        """
        ...

    @typing.overload
    def set_assignments_issuing_config(self, issue_task_suites_in_creation_order: typing.Optional[bool] = None):
        """A shortcut setter for assignments_issuing_config
        """
        ...

    @typing.overload
    def set_filter(self, filter: toloka.client.filter.FilterCondition):
        """A shortcut setter for filter
        """
        ...

    @typing.overload
    def set_filter(self):
        """A shortcut setter for filter
        """
        ...

    @typing.overload
    def set_quality_control(self, quality_control: toloka.client.quality_control.QualityControl):
        """A shortcut setter for quality_control
        """
        ...

    @typing.overload
    def set_quality_control(
        self,
        *,
        training_requirement: typing.Optional[toloka.client.quality_control.QualityControl.TrainingRequirement] = None,
        captcha_frequency: typing.Union[toloka.client.quality_control.QualityControl.CaptchaFrequency, str, None] = None,
        configs: typing.Optional[typing.List[toloka.client.quality_control.QualityControl.QualityControlConfig]] = ...,
        checkpoints_config: typing.Optional[toloka.client.quality_control.QualityControl.CheckpointsConfig] = None
    ):
        """A shortcut setter for quality_control
        """
        ...

    @typing.overload
    def set_speed_quality_balance(self, speed_quality_balance: toloka.client.pool.speed_quality_balance_config.SpeedQualityBalanceConfig):
        """A shortcut setter for speed_quality_balance
        """
        ...

    @typing.overload
    def set_speed_quality_balance(self):
        """A shortcut setter for speed_quality_balance
        """
        ...

    @typing.overload
    def set_dynamic_overlap_config(self, dynamic_overlap_config: toloka.client.pool.dynamic_overlap_config.DynamicOverlapConfig):
        """A shortcut setter for dynamic_overlap_config
        """
        ...

    @typing.overload
    def set_dynamic_overlap_config(
        self,
        *,
        type: typing.Union[toloka.client.pool.dynamic_overlap_config.DynamicOverlapConfig.Type, str, None] = None,
        max_overlap: typing.Optional[int] = None,
        min_confidence: typing.Optional[float] = None,
        answer_weight_skill_id: typing.Optional[str] = None,
        fields: typing.Optional[typing.List[toloka.client.pool.dynamic_overlap_config.DynamicOverlapConfig.Field]] = None
    ):
        """A shortcut setter for dynamic_overlap_config
        """
        ...

    @typing.overload
    def set_mixer_config(self, mixer_config: toloka.client.pool.mixer_config.MixerConfig):
        """A shortcut setter for mixer_config
        """
        ...

    @typing.overload
    def set_mixer_config(
        self,
        *,
        real_tasks_count: int = 0,
        golden_tasks_count: int = 0,
        training_tasks_count: int = 0,
        min_real_tasks_count: typing.Optional[int] = None,
        min_golden_tasks_count: typing.Optional[int] = None,
        min_training_tasks_count: typing.Optional[int] = None,
        force_last_assignment: typing.Optional[bool] = None,
        force_last_assignment_delay_seconds: typing.Optional[int] = None,
        mix_tasks_in_creation_order: typing.Optional[bool] = None,
        shuffle_tasks_in_task_suite: typing.Optional[bool] = None,
        golden_task_distribution_function: typing.Optional[toloka.client.task_distribution_function.TaskDistributionFunction] = None,
        training_task_distribution_function: typing.Optional[toloka.client.task_distribution_function.TaskDistributionFunction] = None
    ):
        """A shortcut setter for mixer_config
        """
        ...

    @typing.overload
    def set_training_config(self, training_config: TrainingConfig):
        """A shortcut setter for training_config
        """
        ...

    @typing.overload
    def set_training_config(self, training_skill_ttl_days: typing.Optional[int] = None):
        """A shortcut setter for training_config
        """
        ...

    @typing.overload
    def set_owner(self, owner: toloka.client.owner.Owner):
        """A shortcut setter for owner
        """
        ...

    @typing.overload
    def set_owner(
        self,
        *,
        id: typing.Optional[str] = None,
        myself: typing.Optional[bool] = None,
        company_id: typing.Optional[str] = None
    ):
        """A shortcut setter for owner
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    project_id: typing.Optional[str]
    private_name: typing.Optional[str]
    may_contain_adult_content: typing.Optional[bool]
    reward_per_assignment: typing.Optional[float]
    assignment_max_duration_seconds: typing.Optional[int]
    defaults: typing.Optional[Defaults]
    will_expire: typing.Optional[datetime.datetime]
    private_comment: typing.Optional[str]
    public_description: typing.Optional[str]
    public_instructions: typing.Optional[str]
    auto_close_after_complete_delay_seconds: typing.Optional[int]
    dynamic_pricing_config: typing.Optional[toloka.client.pool.dynamic_pricing_config.DynamicPricingConfig]
    auto_accept_solutions: typing.Optional[bool]
    auto_accept_period_day: typing.Optional[int]
    assignments_issuing_config: typing.Optional[AssignmentsIssuingConfig]
    priority: typing.Optional[int]
    filter: typing.Optional[toloka.client.filter.FilterCondition]
    quality_control: typing.Optional[toloka.client.quality_control.QualityControl]
    speed_quality_balance: typing.Optional[toloka.client.pool.speed_quality_balance_config.SpeedQualityBalanceConfig]
    dynamic_overlap_config: typing.Optional[toloka.client.pool.dynamic_overlap_config.DynamicOverlapConfig]
    mixer_config: typing.Optional[toloka.client.pool.mixer_config.MixerConfig]
    training_config: typing.Optional[TrainingConfig]
    metadata: typing.Optional[typing.Dict[str, typing.List[str]]]
    owner: typing.Optional[toloka.client.owner.Owner]
    id: typing.Optional[str]
    status: typing.Optional[Status]
    last_close_reason: typing.Optional[CloseReason]
    created: typing.Optional[datetime.datetime]
    last_started: typing.Optional[datetime.datetime]
    last_stopped: typing.Optional[datetime.datetime]
    type: typing.Optional[Type]


class PoolPatchRequest(toloka.client.primitives.base.BaseTolokaObject):
    """New pool parameters.

    This class is used with the [patch_pool](toloka.client.TolokaClient.patch_pool.md) method.

    Attributes:
        priority: The new priority of the pool.
            Possible values: from -100 to 100.
    """

    def __init__(self, priority: typing.Optional[int] = None) -> None:
        """Method generated by attrs for class PoolPatchRequest.
        """
        ...

    _unexpected: typing.Optional[typing.Dict[str, typing.Any]]
    priority: typing.Optional[int]

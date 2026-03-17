__all__ = ['QualityControl']

import logging
from enum import unique
from typing import List

from .actions import RuleAction
from .collectors import CollectorConfig
from .conditions import RuleCondition
from .primitives.base import BaseTolokaObject
from .task_distribution_function import TaskDistributionFunction
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


logger = logging.getLogger(__file__)


def _captcha_deprecation_warning_setter(obj, name, value):
    if value is not None:
        logger.warning(
            'CAPTCHA frequency setting is deprecated and will be removed in future. '
            'CAPTCHAs are now included automatically for better quality control.'
        )


class QualityControl(BaseTolokaObject):
    """Quality control settings.

    Quality control lets you get more accurate responses, restrict access to tasks for Tolokers who give responses of low quality, and filter out robots.

    Attributes:
        configs: A list of quality control rules configurations.
        checkpoints_config: A selective majority vote check configuration.
        training_requirement: Parameters for linking a training pool to a general task pool.
        captcha_frequency: **Deprecated.** A frequency of showing captchas.
            * `LOW` — Show one for every 20 tasks.
            * `MEDIUM`, `HIGH` — Show one for every 10 tasks.

            By default, captchas aren't displayed.

    Example:
        A quality control rule that restricts access if a Toloker responds too fast.

        >>> new_pool = toloka.client.pool.Pool()
        >>> new_pool.quality_control.add_action(
        >>>     collector=toloka.client.collectors.AssignmentSubmitTime(history_size=5, fast_submit_threshold_seconds=20),
        >>>     conditions=[toloka.client.conditions.FastSubmittedCount > 1],
        >>>     action=toloka.client.actions.RestrictionV2(
        >>>         scope=toloka.client.user_restriction.UserRestriction.ALL_PROJECTS,
        >>>         duration=10,
        >>>         duration_unit='DAYS',
        >>>         private_comment='Fast responses',
        >>>     )
        >>> )
        ...
    """

    class TrainingRequirement(BaseTolokaObject):
        """Parameters for linking a training pool to a general task pool.

        Attributes:
            training_pool_id: The ID of the training pool.
            training_passing_skill_value: The percentage of correct answers in training tasks required in order to access the general tasks.
                Only the first answer of the Toloker in each task is taken into account.

                Allowed values: from 0 to 100.
        """

        training_pool_id: str
        training_passing_skill_value: int

    @unique
    class CaptchaFrequency(ExtendableStrEnum):
        LOW = 'LOW'
        MEDIUM = 'MEDIUM'
        HIGH = 'HIGH'

    class CheckpointsConfig(BaseTolokaObject):
        """A selective majority vote check configuration.

        This quality control method checks some of Toloker's responses against the majority of Tolokers. To do this, it changes the overlap of those tasks.

        An example of the configuration:
            * For the first 100 tasks completed by a Toloker in the pool, every 5th task is checked. The overlap of these tasks is increased to 5.
            * After completing 100 tasks, every 25th task is checked.

        Learn more about the [Selective majority vote check](https://toloka.ai/docs/guide/selective-mvote/).

        Attributes:
            real_settings: Selective majority vote settings for general tasks.
            golden_settings: Selective majority vote settings for control tasks.
            training_settings: Selective majority vote settings for training tasks.
        """

        class Settings(BaseTolokaObject):
            """Selective majority vote check settings.

            Attributes:
                target_overlap: The overlap value used for selected tasks that are checked.
                task_distribution_function: The configuration of selecting tasks.
            """

            target_overlap: int
            task_distribution_function: TaskDistributionFunction

        real_settings: Settings
        golden_settings: Settings
        training_settings: Settings

    class QualityControlConfig(BaseTolokaObject):
        """A quality control rules configuration.

        A rule consists of conditions, and an action to perform when the conditions are met. The rule conditions use statistics provided by a connected collector.

        An example of the configuration.
        Toloka collects statistics of skipped tasks. If 10 task suites are skipped in a row, then a Toloker can no longer access a project.

        To learn more, see:
        * [Quality control rules](https://toloka.ai/docs/api/quality_control/) in the API.
        * [Quality control rules](https://toloka.ai/docs/guide/control/) in the guide.

        Attributes:
            rules: The conditions and the action.
            collector_config: The configuration of the collector.
        """

        class RuleConfig(BaseTolokaObject):
            """Rule conditions and an action.

            The action is performed if conditions are met. Multiple conditions are combined with the AND operator.

            Attributes:
                action: The action.
                conditions: A list of conditions.
            """

            action: RuleAction
            conditions: List[RuleCondition]

        rules: List[RuleConfig]
        collector_config: CollectorConfig

    training_requirement: TrainingRequirement
    captcha_frequency: CaptchaFrequency = attribute(autocast=True, on_setattr=_captcha_deprecation_warning_setter)
    configs: List[QualityControlConfig] = attribute(factory=list)
    checkpoints_config: CheckpointsConfig

    def add_action(self, collector: CollectorConfig, action: RuleAction, conditions: List[RuleCondition]):
        """Adds a quality control rule configuration.

        See an example in the description of the [QualityControl](toloka.client.quality_control.QualityControl.md) class.

        Args:
            collector: A collector that provides statistics.
            conditions: Conditions based on statistics.
            action: An action performed if all conditions are met.
        """

        # Checking that conditions are compatible with our collector
        collector.validate_condition(conditions)

        # We can possibly add the action to an existing config
        for config in self.configs:
            if config.collector_config == collector:
                break
        else:
            config = QualityControl.QualityControlConfig(rules=[], collector_config=collector)
            self.configs.append(config)

        rule = QualityControl.QualityControlConfig.RuleConfig(action=action, conditions=conditions)
        config.rules.append(rule)

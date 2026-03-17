__all__ = [
    'SpeedQualityBalanceConfig',
    'TopPercentageByQuality',
    'BestConcurrentUsersByQuality',
]
from enum import unique
from ..primitives.base import BaseTolokaObject
from ...util._extendable_enum import ExtendableStrEnum


class SpeedQualityBalanceConfig(BaseTolokaObject, spec_enum='Type', spec_field='type'):
    """A configuration of selecting Tolokers based on a personalized quality forecast.

    Tolokers are sorted by their quality forecast. You can limit the number of the best Tolokers who have access to your tasks.
    It influences quality of results and speed of getting results.

    Learn more about [Speed/quality balance](https://toloka.ai/docs/guide/adjust).
    """

    @unique
    class Type(ExtendableStrEnum):
        """The type of the filter used in [SpeedQualityBalanceConfig](toloka.client.pool.speed_quality_balance_config.SpeedQualityBalanceConfig.md).

        Attributes:
            TOP_PERCENTAGE_BY_QUALITY: A percentage of the best Tolokers is configured.
            BEST_CONCURRENT_USERS_BY_QUALITY: A maximum number of the best Tolokers is configured.
        """
        TOP_PERCENTAGE_BY_QUALITY = 'TOP_PERCENTAGE_BY_QUALITY'
        BEST_CONCURRENT_USERS_BY_QUALITY = 'BEST_CONCURRENT_USERS_BY_QUALITY'


class TopPercentageByQuality(SpeedQualityBalanceConfig,
                             spec_value=SpeedQualityBalanceConfig.Type.TOP_PERCENTAGE_BY_QUALITY):
    """`SpeedQualityBalanceConfig` that uses percentage of Tolokers.
    """
    percent: int


class BestConcurrentUsersByQuality(SpeedQualityBalanceConfig,
                                   spec_value=SpeedQualityBalanceConfig.Type.BEST_CONCURRENT_USERS_BY_QUALITY):
    """`SpeedQualityBalanceConfig` that uses a maximum number of Tolokers.
    """
    count: int

from datetime import timedelta
import logging
from typing import List

from .. import mapping, pricing

logger = logging.getLogger(__name__)


# some libs can do it, but we don't want to add dependency just because of this function
def ask(question: str) -> bool:
    answer = input(f'{question} [Y/n] ')
    return answer.lower() == 'y'


def ask_or_warn(msg: str, interactive: bool) -> bool:
    if interactive:
        return ask(msg)
    logger.warning(msg)
    return True


OBJECTS_COUNT_VALID_RANGE = (1, 100000)
OBJECTS_COUNT_RECOMMENDED_RANGE = (1, 10000)
OBJECTS_DURATION_RECOMMENDED_RANGE = (timedelta(minutes=1), timedelta(days=2))
CONTROL_OBJECTS_RECOMMENDED_MIN_RATIO = 0.05
RECOMMENDED_RANGE_MESSAGE = (
    'Too small objects volume is inefficient in terms of speed, too big may cause problems '
    'in crowd-sourcing platform API or can be risky if launch is misconfigured'
)


def validate_objects_volume(
    input_objects: List[mapping.Objects],
    control_objects: List[mapping.TaskSingleSolution],  # assisted by us
    task_duration_hint: timedelta,
    pricing_config: pricing.PoolPricingConfig,
    interactive: bool = False,
) -> bool:
    no_control_objects = pricing_config.control_tasks_count == 0

    if not control_objects:
        assert no_control_objects, 'You should provide control objects'

    control_objects_valid_range = (
        max(OBJECTS_COUNT_VALID_RANGE[0], pricing_config.control_tasks_count),
        OBJECTS_COUNT_VALID_RANGE[1],
    )
    control_objects_recommended_range = (
        max(OBJECTS_COUNT_RECOMMENDED_RANGE[0], pricing_config.control_tasks_count),
        OBJECTS_COUNT_RECOMMENDED_RANGE[1],
    )
    control_objects_recommended_duration_range = OBJECTS_DURATION_RECOMMENDED_RANGE
    control_objects_recommended_min_ratio = CONTROL_OBJECTS_RECOMMENDED_MIN_RATIO

    if no_control_objects:
        control_objects_valid_range = control_objects_recommended_range = (0, 0)
        control_objects_recommended_duration_range = (timedelta(0), timedelta(0))
        control_objects_recommended_min_ratio = 0.0

    question = 'Do you wish to continue?'

    for objects, desc, valid_range, recommended_range, duration_recommended_range in (
        (
            input_objects,
            'Objects',
            OBJECTS_COUNT_VALID_RANGE,
            OBJECTS_COUNT_RECOMMENDED_RANGE,
            OBJECTS_DURATION_RECOMMENDED_RANGE,
        ),
        (
            control_objects,
            'Control objects',
            control_objects_valid_range,
            control_objects_recommended_range,
            control_objects_recommended_duration_range,
        ),
    ):
        assert valid_range[0] <= len(objects) <= valid_range[1], (
            f'{desc} count must be in range [{valid_range[0]}, {valid_range[1]}], ' f'you have {len(objects)}'
        )

        if not (recommended_range[0] <= len(objects) <= recommended_range[1]):
            msg = (
                f'{desc} count recommended range is [{recommended_range[0]}, {recommended_range[1]}], '
                f'you have {len(objects)}. {RECOMMENDED_RANGE_MESSAGE}. {question}'
            )
            if not ask_or_warn(msg, interactive):
                return False

        if not (duration_recommended_range[0] <= task_duration_hint * len(objects) <= duration_recommended_range[1]):
            msg = (
                f'{desc} duration recommended range is '
                f'[{duration_recommended_range[0]} - {duration_recommended_range[1]}], '
                f'you have {task_duration_hint * len(objects)}. {RECOMMENDED_RANGE_MESSAGE}. {question}'
            )
            if not ask_or_warn(msg, interactive):
                return False

    if len(control_objects) / len(input_objects) < control_objects_recommended_min_ratio:
        msg = (
            f'Recommended control objects minimum ratio is {CONTROL_OBJECTS_RECOMMENDED_MIN_RATIO}, you have '
            f'{len(control_objects) / len(input_objects):.2f}. A sufficient control tasks ratio is needed for '
            f'higher throughput (each worker will see specific control task only once). {question}'
        )
        if not ask_or_warn(msg, interactive):
            return False

    return True

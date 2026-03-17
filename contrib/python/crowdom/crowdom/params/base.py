from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import List, Dict, Union, Optional


class QualityPresetType(Enum):
    NO_CONTROL = 'No control'
    MILD = 'Mild'
    MODERATE = 'Moderate'
    STRICT = 'Strict'
    CUSTOM = 'Custom'


OptionType = Union[str, int, float, bool, timedelta, QualityPresetType, List['OptionType']]


class ParamName(Enum):
    HELP = 'help'
    CHARACTERISTICS = 'characteristics'
    TASK_DURATION_HINT = 'task_duration_hint'
    SPEED_CONTROL = 'speed_control'
    LANGUAGE = 'lang'
    TASK_COUNT = 'task_count'
    ASSIGNMENT_PRICE = 'assignment_price'
    QUALITY_PRESET = 'quality_preset'
    CONTROL_TASK_COUNT = 'control_task_count'
    CONTROL_TASKS_FOR_ACCEPT = 'control_tasks_for_accept'
    CONTROL_TASKS_FOR_BLOCK = 'control_tasks_for_block'
    TRAINING = 'training'
    TRAINING_SCORE = 'training_score'
    AGE_RESTRICTION = 'age_restriction'
    VERIFIED_LANG = 'verified_lang'
    OVERLAP = 'overlap'
    OVERLAP_CONFIDENCE = 'overlap_confidence'
    AGGREGATION_ALGORITHM = 'aggregation_algorithm'
    QUALITY = 'quality'
    TIME = 'time'
    COST = 'cost'
    GENERAL_QUALITY_GROUP = 'general_quality_group'
    QUALITY_TASKS_GROUP = 'quality_tasks_group'
    QUALITY_ASSIGNMENT_GROUP = 'quality_assignment_group'
    CHECK_SAMPLE_GROUP = 'check_sample_group'
    STATISTICS = 'statistics'
    CHECK_SAMPLE = 'check_sample'
    MAX_TASKS_TO_CHECK = 'max_tasks_to_check'
    ACCURACY_THRESHOLD = 'accuracy_threshold'

    @property
    def ui(self) -> str:
        return f'ui-{self.value}'


@dataclass
class Update:
    value: OptionType
    disabled: bool = False


@dataclass
class Event:
    sender: str
    update: Dict[ParamName, Update]


@dataclass
class UIEvent(Event):
    ...


class SpeedControlChangeType(Enum):
    ADD = 'add'
    REMOVE = 'remove'
    RATIO = 'ratio'
    REJECT = 'reject'
    TIME = 'time_value'


@dataclass
class SpeedControlUpdate(Update):
    change_type: SpeedControlChangeType = SpeedControlChangeType.ADD
    index: Optional[int] = None


@dataclass
class SpeedControlEvent(UIEvent):
    update: Dict[SpeedControlChangeType, SpeedControlUpdate]


@dataclass
class Node:
    name: ParamName

    def process(self, event: Event) -> List[Event]:
        return []

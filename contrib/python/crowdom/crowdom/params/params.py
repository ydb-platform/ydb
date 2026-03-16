from copy import copy
from dataclasses import dataclass, field
from datetime import timedelta
from math import ceil
from typing import List, Tuple, Optional

import pandas as pd
import toloka.client as toloka
from toloka.client.filter import Languages


from .. import experts, task_spec as spec
from ..pricing import (
    _get_assignment_params,
    _get_random_chance,
    min_assignment_cost,
    min_tasks_in_assignment,
    max_tasks_in_assignment,
    threshold_for_assignment_cost_granularity as threshold_cost,
    round_price,
)
from ..control import BlockTimePicker

from .base import (
    OptionType,
    Node,
    ParamName,
    Update,
    Event,
    UIEvent,
    SpeedControlEvent,
    SpeedControlChangeType,
    QualityPresetType,
)
from .util import get_overlap, overlap_is_static


default_block = BlockTimePicker(0.05, '30m', False)


@dataclass
class Param(Node):
    description: str
    value: OptionType
    available_range: List[OptionType]
    recommended_range: List[OptionType]
    help_msg: str
    disabled: bool = False

    def process(self, event: Event) -> List[Event]:
        if self.name in event.update:
            self.value, self.disabled = event.update[self.name].value, event.update[self.name].disabled
        return []


@dataclass
class ParamGroup(Node):
    params: List[Param]

    def recalculate(self, event: Event) -> Event:
        return event

    def process(self, event: Event) -> List[Event]:
        update = event.update

        for p in self.params:
            if p.name not in update:
                update[p.name] = Update(p.value, p.disabled)

        event = Event(
            sender=self.name.value,
            update=update,
        )
        return [self.recalculate(event)]


@dataclass
class ParamGroupControlTasksRecalculate(ParamGroup):
    def recalculate(self, event: Event) -> Event:
        task_count = event.update[ParamName.TASK_COUNT].value
        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        control_task_count = event.update[ParamName.CONTROL_TASK_COUNT].value
        control_task_param = next(p for p in self.params if p.name == ParamName.CONTROL_TASK_COUNT)
        value, _, _, disabled = control_task_param.get_values(task_count, quality_preset, control_task_count)
        control_task_count_update = Update(value, disabled)
        new_update = copy(event.update)
        new_update[ParamName.CONTROL_TASK_COUNT] = control_task_count_update
        new_event = Event(sender=event.sender, update=new_update)
        return new_event


@dataclass
class ParamGroupCheckedTasksRecalculate(ParamGroup):
    def recalculate(self, event: Event) -> Event:
        task_count = event.update[ParamName.TASK_COUNT].value
        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        check_sample = event.update[ParamName.CHECK_SAMPLE].value
        max_tasks_to_check = event.update[ParamName.MAX_TASKS_TO_CHECK].value
        max_tasks_to_check_param = next(p for p in self.params if p.name == ParamName.MAX_TASKS_TO_CHECK)
        value, _, _, disabled = max_tasks_to_check_param.get_values(
            quality_preset, task_count, check_sample, max_tasks_to_check
        )
        max_tasks_to_check_update = Update(value, disabled)
        new_update = copy(event.update)
        new_update[ParamName.MAX_TASKS_TO_CHECK] = max_tasks_to_check_update
        new_event = Event(sender=event.sender, update=new_update)
        return new_event


@dataclass
class Help(Param):
    name: ParamName = ParamName.HELP
    description: str = ''
    value: str = '<b>Help for interface elements will be here</b>'
    available_range: List[OptionType] = field(default_factory=list)
    recommended_range: List[OptionType] = field(default_factory=list)
    help_msg: str = ''

    def process(self, event: Event) -> List[Event]:
        assert event.sender == self.name.value
        self.value = event.update[self.name].value
        return []


@dataclass
class Stat(Param):
    value: float = 0.0
    available_range: List[OptionType] = field(default_factory=list)
    recommended_range: List[OptionType] = field(default_factory=list)
    help_msg: str = ''
    reverse: bool = False


@dataclass
class StatWithExtraText(Stat):
    value: Tuple[float, str] = 0.0, ''


@dataclass
class Characteristics(Param):
    value: List[str] = field(default_factory=list)
    name: ParamName = ParamName.CHARACTERISTICS
    description: Optional[str] = None
    available_range: List[OptionType] = field(default_factory=list)
    recommended_range: List[OptionType] = field(default_factory=list)
    help_msg: str = ''


@dataclass(init=False)
class TaskDuration(Param):
    name: ParamName = ParamName.TASK_DURATION_HINT
    description: str = 'Task duration hint, sec'
    value: timedelta = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'This is task duration hint you provided (in seconds)'
    disabled: bool = True

    def __init__(self, task_duration_hint: timedelta):
        self.value = task_duration_hint
        self.available_range = [1, max(3 * 60, int(task_duration_hint.total_seconds()))]
        self.recommended_range = [5, 60]


@dataclass(init=False)
class SpeedControlList(Param):
    name: ParamName = ParamName.SPEED_CONTROL
    description: str = 'Speed control'
    value: List[BlockTimePicker] = field(init=False)
    available_range: Optional[list] = None
    recommended_range: Optional[list] = None
    help_msg: str = (
        'If workers complete your assignments suspiciously fast, you can ban them for some time and reject'
        ' these assignments. '
        'Threshold specifies percentage of time spent on task out of <i>task duration hint</i> total.'
    )
    disabled: bool = True
    list_size: int = 5

    min_value: int = pd.Timedelta('10m').total_seconds()
    max_value: int = pd.Timedelta('30d').total_seconds()

    @staticmethod
    def get_values(
        quality_preset: str, items: Optional[List[BlockTimePicker]] = None
    ) -> Tuple[List[BlockTimePicker], bool]:
        if quality_preset == QualityPresetType.NO_CONTROL.value:
            return [], True
        if quality_preset == QualityPresetType.CUSTOM.value:
            return (items or []), False
        speed_poor, speed_fraud = {
            QualityPresetType.MILD.value: (0.05, 0.01),
            QualityPresetType.MODERATE.value: (0.3, 0.1),
            QualityPresetType.STRICT.value: (0.5, 0.3),
        }[quality_preset]
        return [
            BlockTimePicker(speed_fraud, '1d', True),
            BlockTimePicker(speed_poor, '2h', False),
        ], True

    def __init__(self, quality_preset: str, list_size: int = 5):
        self.value, self.disabled = self.get_values(quality_preset)
        self.list_size = list_size

    def check_update(self, event: SpeedControlEvent) -> bool:
        for name, update in event.update.items():
            if (
                name in [SpeedControlChangeType.ADD, SpeedControlChangeType.REMOVE]
                or getattr(self.value[update.index], name.value) != update.value
            ):
                return True
        return False

    def process(self, event: Event) -> List[Event]:
        if event.sender == ParamName.GENERAL_QUALITY_GROUP.value:
            self.value, self.disabled = self.get_values(event.update[ParamName.QUALITY_PRESET].value, self.value)
            return []

        assert event.sender == self.name.ui
        assert len(event.update) == 1

        [(name, update)] = event.update.items()

        if name == SpeedControlChangeType.ADD:
            self.value.append(copy(default_block))
            return []

        index = update.index

        if name == SpeedControlChangeType.REMOVE:
            self.value.pop(index)
        else:
            setattr(self.value[index], name.value, update.value)
        return []


@dataclass(init=False)
class Lang(Param):
    name: ParamName = ParamName.LANGUAGE
    description: str = 'Language'
    value: str = field(init=False)
    available_range: List[str] = field(init=False)
    recommended_range: List[str] = field(init=False)
    help_msg: str = 'This is source language you provided for this parameters optimization'
    disabled: bool = True

    def __init__(self, task_spec: spec.PreparedTaskSpec):
        self.value = task_spec.lang
        self.available_range = [self.value]
        self.recommended_range = ['EN', 'RU']


@dataclass(init=False)
class TaskCount(Param):
    name: ParamName = ParamName.TASK_COUNT
    description: str = 'Task count'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'Number of real tasks per assignment'

    def __init__(self, task_duration_hint: timedelta):
        max_task_count_calc = ceil(5 * 60 / task_duration_hint.total_seconds())
        min_task_count_calc = ceil(30 / task_duration_hint.total_seconds())

        min_task_count = max(min_tasks_in_assignment, min_task_count_calc)
        max_task_count = min(max_tasks_in_assignment // 2, max_task_count_calc)

        assert min_task_count <= max_task_count

        self.value = (min_task_count + max_task_count) // 2
        self.available_range = [min_tasks_in_assignment, max_tasks_in_assignment]
        self.recommended_range = [min_task_count, max_task_count]


def get_price_step(assignment_price: float) -> float:
    if assignment_price > threshold_cost:
        return 0.01
    return 0.001


def get_min_price(assignment_price: float) -> float:
    if assignment_price > threshold_cost:
        return 0.01
    return min_assignment_cost


@dataclass(init=False)
class AssignmentPrice(Param):
    name: ParamName = ParamName.ASSIGNMENT_PRICE
    description: str = 'Assignment price, $'
    value: float = field(init=False)
    available_range: List[float] = field(init=False)
    recommended_range: List[float] = field(init=False)
    help_msg: str = 'Base price of assignment that workers will receive, doesn\'t include Toloka commission'

    @staticmethod
    def get_values(
        task_duration_hint: timedelta,
        task_count: int,
        control_task_count: int,
    ):
        price_per_hour_range = [0.1, 2.5]
        default_price_per_hour_recommended_range = [0.3, 0.9]

        min_price = min_assignment_cost
        _, max_price, _ = _get_assignment_params(
            max_tasks_in_assignment + get_max_control_tasks_count(max_tasks_in_assignment),
            task_duration_hint,
            price_per_hour_range[-1],
        )

        _, min_rec_price, _ = _get_assignment_params(
            task_count + control_task_count,
            task_duration_hint,
            default_price_per_hour_recommended_range[0],
        )
        _, max_rec_price, _ = _get_assignment_params(
            task_count + control_task_count,
            task_duration_hint,
            default_price_per_hour_recommended_range[-1],
        )
        value = round_price((min_rec_price + max_rec_price) / 2)

        # we need to adjust min bound for parameter, so that current value can be represented as min + k * step
        # for prices above 1$ step is 0.01, so new min_assignment_cost=0.005 will break the logic
        min_price = max(min_price, get_min_price(value))
        return value, [min_price, max_price], [min_rec_price, max_rec_price]

    def __init__(self, task_duration_hint, task_count: int, control_task_count: int):
        self.value, self.available_range, self.recommended_range = self.get_values(
            task_duration_hint, task_count, control_task_count
        )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            if self.name in event.update:
                value, self.disabled = event.update[self.name].value, event.update[self.name].disabled
                self.available_range[0] = get_min_price(value)
                # we need to calculate step based on some value that is greater that threshold_cost
                step = get_price_step(threshold_cost + 1)
                if threshold_cost < value < threshold_cost + step:
                    # edge effect due to only 1 step value allowed for widget
                    # we catch "incorrect" increase here and automatically fix it
                    value = threshold_cost + step

                self.value = round_price(value)
            return []
        task_count = event.update[ParamName.TASK_COUNT].value
        control_task_count = event.update[ParamName.CONTROL_TASK_COUNT].value
        task_duration_hint = event.update[ParamName.TASK_DURATION_HINT].value

        self.value, self.available_range, self.recommended_range = self.get_values(
            task_duration_hint, task_count, control_task_count
        )
        return []


class AnnotationAssignmentPrice(AssignmentPrice):
    @staticmethod
    def get_values(
        task_duration_hint: timedelta,
        task_count: int,
    ):
        price_per_hour_range = [0.1, 2.5]
        default_price_per_hour_recommended_range = [0.3, 0.9]

        min_price = min_assignment_cost
        _, max_price, _ = _get_assignment_params(
            max_tasks_in_assignment,
            task_duration_hint,
            price_per_hour_range[-1],
        )

        _, min_rec_price, _ = _get_assignment_params(
            task_count,
            task_duration_hint,
            default_price_per_hour_recommended_range[0],
        )
        _, max_rec_price, _ = _get_assignment_params(
            task_count,
            task_duration_hint,
            default_price_per_hour_recommended_range[-1],
        )
        value = round_price((min_rec_price + max_rec_price) / 2)
        min_price = max(min_price, get_min_price(value))
        return value, [min_price, max_price], [min_rec_price, max_rec_price]

    def __init__(self, task_duration_hint, task_count: int):
        # todo: maybe we need to take # of checks per page into account
        self.value, self.available_range, self.recommended_range = self.get_values(task_duration_hint, task_count)

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)
        task_count = event.update[ParamName.TASK_COUNT].value
        task_duration_hint = event.update[ParamName.TASK_DURATION_HINT].value

        self.value, self.available_range, self.recommended_range = self.get_values(
            task_duration_hint,
            task_count,
        )
        return []


@dataclass
class QualityPreset(Param):
    name: ParamName = ParamName.QUALITY_PRESET
    description: str = 'Quality control preset'
    value: str = QualityPresetType.MODERATE.value
    available_range: List[str] = field(default_factory=lambda: [e.value for e in QualityPresetType])
    recommended_range: List[str] = field(
        default_factory=lambda: [QualityPresetType.MODERATE.value, QualityPresetType.STRICT.value]
    )
    help_msg: str = 'General quality control presets, that will manage all the options for you'


@dataclass
class AnnotationQualityPreset(QualityPreset):
    available_range: List[str] = field(
        default_factory=lambda: [e.value for e in QualityPresetType if e != QualityPresetType.NO_CONTROL]
    )


def get_max_control_tasks_count(task_count: int) -> int:
    return task_count


def get_checked_tasks_count(task_count: int, check_sample: bool, max_tasks_to_check: int) -> int:
    if not check_sample:
        return task_count
    assert max_tasks_to_check <= task_count
    return max_tasks_to_check


@dataclass(init=False)
class ControlTaskCount(Param):
    name: ParamName = ParamName.CONTROL_TASK_COUNT
    description: str = 'Control task count'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'Number of control tasks per assignment'

    @staticmethod
    def get_values(
        task_count: int,
        quality_preset: str,
        control_task_count: Optional[int] = None,
    ):
        control_task_count_rec_range = [1, ceil(0.25 * task_count)]
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            control_task_ratio = {
                QualityPresetType.NO_CONTROL.value: 0.0,
                QualityPresetType.MILD.value: 0.1,
                QualityPresetType.MODERATE.value: 0.2,
                QualityPresetType.STRICT.value: 0.4,
            }[quality_preset]

            control_task_count = ceil(control_task_ratio * task_count)

            # this is separate logic from get_max_control_tasks_count:
            # in this case, some preset is chosen, and some control_task_count is created automatically,
            # but we want to set an upper-bound to number of control tasks per page
            control_task_count_range = [0, max(control_task_count, min(10, task_count))]

        else:
            if control_task_count is None:
                control_task_count = (control_task_count_rec_range[0] + control_task_count_rec_range[-1]) // 2
            control_task_count_range = [0, get_max_control_tasks_count(task_count)]

        control_task_count_rec_range[-1] = min(control_task_count_rec_range[-1], control_task_count_range[-1])

        control_task_count = max(control_task_count, control_task_count_range[0])
        control_task_count = min(control_task_count, control_task_count_range[-1])

        return control_task_count, control_task_count_range, control_task_count_rec_range, disabled

    def __init__(self, task_count: int, quality_preset: str):
        self.value, self.available_range, self.recommended_range, self.disabled = self.get_values(
            task_count,
            quality_preset,
        )

    def process(self, event: Event) -> List[Event]:
        task_count = event.update[ParamName.TASK_COUNT].value
        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        control_task_count = event.update[ParamName.CONTROL_TASK_COUNT].value
        self.value, self.available_range, self.recommended_range, self.disabled = self.get_values(
            task_count, quality_preset, control_task_count
        )
        return []


@dataclass(init=False)
class ControlTasksForAccept(Param):
    name: ParamName = ParamName.CONTROL_TASKS_FOR_ACCEPT
    description: str = 'Accept, if correct control tasks &ge;'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = (
        'If worker completed correctly this many control tasks of more in the assignment, '
        'this assignment will be accepted; otherwise - rejected'
    )

    num_classes: int = field(init=False)

    @staticmethod
    def get_values(
        num_classes: int,
        quality_preset: str,
        control_task_count: int,
        control_tasks_for_accept: Optional[int] = None,
    ) -> Tuple[int, bool, List[int], List[int]]:
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            robustness_ = {
                QualityPresetType.NO_CONTROL.value: 0.0,
                QualityPresetType.MILD.value: 0.6,
                QualityPresetType.MODERATE.value: 0.75,
                QualityPresetType.STRICT.value: 0.9,
            }[quality_preset]

            for control_tasks_for_accept in range(control_task_count + 1):
                robustness = 1.0 - _get_random_chance(control_task_count, control_tasks_for_accept, num_classes)
                if robustness >= robustness_:
                    break
        control_tasks_for_accept = min(control_task_count, control_tasks_for_accept)
        control_task_for_accept_range = [0, control_task_count]
        control_task_for_accept_rec_range = [ceil(0.2 * control_task_count), ceil(0.6 * control_task_count)]
        return control_tasks_for_accept, disabled, control_task_for_accept_range, control_task_for_accept_rec_range

    def __init__(self, num_classes: int, quality_preset: str, control_task_count: int):
        self.num_classes = num_classes
        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            num_classes,
            quality_preset,
            control_task_count,
        )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        control_task_count = event.update[ParamName.CONTROL_TASK_COUNT].value

        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            self.num_classes,
            quality_preset,
            control_task_count,
            self.value,
        )
        return []


class CheckedTasksForAccept(ControlTasksForAccept):
    description: str = 'Accept, if correctly done checked tasks &ge;'
    num_classes: Optional[int] = None

    @staticmethod
    def get_values(
        num_classes: Optional[int],  # None, not used here, left for compatibility
        quality_preset: str,
        control_task_count: int,
        control_tasks_for_accept: Optional[int] = None,
    ) -> Tuple[int, bool, List[int], List[int]]:
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            ratio = {
                QualityPresetType.NO_CONTROL.value: 0.0,
                QualityPresetType.MILD.value: 0.6,
                QualityPresetType.MODERATE.value: 0.75,
                QualityPresetType.STRICT.value: 0.9,
            }[quality_preset]
            control_tasks_for_accept = ceil(ratio * control_task_count)

        control_tasks_for_accept = min(control_task_count, control_tasks_for_accept)
        control_task_for_accept_range = [0, control_task_count]
        control_task_for_accept_rec_range = [ceil(0.2 * control_task_count), ceil(0.6 * control_task_count)]
        return control_tasks_for_accept, disabled, control_task_for_accept_range, control_task_for_accept_rec_range

    def __init__(self, quality_preset: str, control_task_count: int):
        super().__init__(None, quality_preset, control_task_count)

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        task_count = event.update[ParamName.TASK_COUNT].value
        max_tasks_to_check = event.update[ParamName.MAX_TASKS_TO_CHECK].value
        check_sample = event.update[ParamName.CHECK_SAMPLE].value
        control_task_count = get_checked_tasks_count(task_count, check_sample, max_tasks_to_check)

        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            None,  # todo for compatibility reasons
            quality_preset,
            control_task_count,
            self.value,
        )
        return []


@dataclass(init=False)
class ControlTasksForBlock(Param):
    name: ParamName = ParamName.CONTROL_TASKS_FOR_BLOCK
    description: str = 'Block, if correct control tasks &le;'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = (
        'If worker completed correctly this many control tasks or less in the assignment, '
        'they would be temporarily blocked due to bad performance'
    )

    @staticmethod
    def get_values(
        quality_preset: str,
        control_task_count: int,
        control_tasks_for_block: Optional[int] = None,
    ):
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            block_ratio = {
                QualityPresetType.NO_CONTROL.value: 0.0,
                QualityPresetType.MILD.value: 0.1,
                QualityPresetType.MODERATE.value: 0.3,
                QualityPresetType.STRICT.value: 0.5,
            }[quality_preset]

            control_tasks_for_block = ceil(block_ratio * control_task_count)
        control_tasks_for_block = min(control_task_count, control_tasks_for_block)
        control_task_for_block_range = [0, control_task_count]
        control_task_for_block_rec_range = [0, ceil(0.3 * control_task_count)]

        return control_tasks_for_block, disabled, control_task_for_block_range, control_task_for_block_rec_range

    def __init__(self, quality_preset: str, control_task_count: int):
        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset,
            control_task_count,
        )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        control_task_count = event.update[ParamName.CONTROL_TASK_COUNT].value

        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset,
            control_task_count,
            self.value,
        )
        return []


class CheckedTasksForBlock(ControlTasksForBlock):
    description: str = 'Block, if correctly done checked tasks &le;'

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        task_count = event.update[ParamName.TASK_COUNT].value
        max_tasks_to_check = event.update[ParamName.MAX_TASKS_TO_CHECK].value
        check_sample = event.update[ParamName.CHECK_SAMPLE].value
        control_task_count = get_checked_tasks_count(task_count, check_sample, max_tasks_to_check)

        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset,
            control_task_count,
            self.value,
        )
        return []


@dataclass(init=False)
class Training(Param):
    name: ParamName = ParamName.TRAINING
    description: str = 'Training'
    value: bool = field(init=False)
    available_range: List[bool] = field(init=False)
    recommended_range: List[bool] = field(init=False)
    help_msg: str = 'Only workers that passed your training will be able to see your task'

    def __init__(self, task_spec: spec.PreparedTaskSpec, toloka_client: toloka.TolokaClient):
        has_training = False
        try:
            from .. import client  # TODO: circular import hotfix
            result = client.find_project(task_spec, toloka_client)
            if result is not None:
                prj, _ = result
                training = experts.find_training(prj.id, toloka_client)
                has_training = training is not None
        except:
            pass
        if not has_training:
            self.help_msg = self.help_msg + '. You don\'t have any available trainings for this task'
        self.value = has_training
        self.disabled = not has_training
        self.available_range = [False, True]
        self.recommended_range = [True]

    def process(self, event: Event) -> List[Event]:
        return super().process(event) + [Event(sender=self.name.value, update={self.name: event.update[self.name]})]


@dataclass(init=False)
class CheckSample(Param):
    name: ParamName = ParamName.CHECK_SAMPLE
    description: str = 'Check sample'
    value: bool = field(init=False)
    available_range: List[bool] = field(init=False)
    recommended_range: List[bool] = field(init=False)
    help_msg: str = 'We can check only a subsample of submitted annotations, which cuts down cost of labeling'

    @staticmethod
    def get_values(quality_preset: str, check_sample: Optional[bool] = None) -> Tuple[bool, bool]:
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            value = False
        else:
            value = check_sample
        return value, disabled

    def __init__(self, quality_preset: str):
        self.value, self.disabled = self.get_values(quality_preset)
        self.available_range = [False, True]
        self.recommended_range = [False]

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        check_sample = event.update[self.name].value
        self.value, self.disabled = self.get_values(quality_preset, check_sample)
        return []


@dataclass(init=False)
class MaxTasksToCheck(Param):
    name: ParamName = ParamName.MAX_TASKS_TO_CHECK
    description: str = 'Max tasks to check'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'Maximum number of annotation tasks that would be checked from each assignment'

    @staticmethod
    def get_values(
        quality_preset: str, task_count: int, check_sample: bool, max_tasks_to_check: Optional[int] = None
    ) -> Tuple[int, bool, List[int], List[int]]:
        disabled = quality_preset != QualityPresetType.CUSTOM.value or not check_sample
        available_range = [1, task_count]
        recommended_range = [task_count]

        if disabled:
            value = task_count
        else:
            value = max_tasks_to_check
        return value, disabled, available_range, recommended_range

    def __init__(self, quality_preset: str, task_count: int, check_sample: bool):
        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset, task_count, check_sample
        )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)
        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        task_count = event.update[ParamName.TASK_COUNT].value
        check_sample = event.update[ParamName.CHECK_SAMPLE].value
        max_tasks_to_check = event.update[self.name].value
        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset,
            task_count,
            check_sample,
            max_tasks_to_check,
        )
        return []


@dataclass(init=False)
class AccuracyThreshold(Param):
    name: ParamName = ParamName.ACCURACY_THRESHOLD
    description: str = 'Accuracy threshold'
    value: int = field(init=False)
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'Threshold number of correctly done tasks in assignment to finalize it'

    @staticmethod
    def get_values(
        quality_preset: str,
        task_count: int,
        check_sample: bool,
        max_tasks_to_check: int,
        accuracy_threshold: Optional[int] = None,
    ) -> Tuple[int, bool, List[int], List[int]]:
        disabled = quality_preset != QualityPresetType.CUSTOM.value or not check_sample
        max_task_count = get_checked_tasks_count(task_count, check_sample, max_tasks_to_check)
        if disabled:
            value = max_task_count
        else:
            value = accuracy_threshold
        available_range = [1, max_task_count]
        recommended_range = [ceil(0.75 * max_task_count), ceil(0.95 * max_task_count)]

        return value, disabled, available_range, recommended_range

    def __init__(self, quality_preset: str, task_count: int, check_sample: bool, max_tasks_to_check: int):
        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset, task_count, check_sample, max_tasks_to_check
        )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)
        quality_preset = event.update[ParamName.QUALITY_PRESET].value
        task_count = event.update[ParamName.TASK_COUNT].value
        check_sample = event.update[ParamName.CHECK_SAMPLE].value
        max_tasks_to_check = event.update[ParamName.MAX_TASKS_TO_CHECK].value

        self.value, self.disabled, self.available_range, self.recommended_range = self.get_values(
            quality_preset, task_count, check_sample, max_tasks_to_check, self.value
        )
        return []


@dataclass(init=False)
class TrainingValue(Param):
    name: ParamName = ParamName.TRAINING_SCORE
    description: str = 'Training score'
    value: int = 80
    available_range: List[int] = field(init=False)
    recommended_range: List[int] = field(init=False)
    help_msg: str = 'Percentage of training that should be completed correctly to qualify as "passed"'

    def __init__(self, training: bool):
        self.disabled = not training
        self.available_range = [0, 100]
        self.recommended_range = [75, 95]

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        self.disabled = not event.update[ParamName.TRAINING].value
        return []


@dataclass(init=False)
class AgeRestriction(Param):
    name: ParamName = ParamName.AGE_RESTRICTION
    description: str = '18+'
    value: bool = True
    available_range: List[bool] = field(init=False)
    recommended_range: List[bool] = field(init=False)
    help_msg: str = (
        'If there may be something inappropriate in your tasks, you should only show them to mature audience'
    )

    def __init__(self):
        self.available_range = [False, True]
        self.recommended_range = [True]


@dataclass(init=False)
class VerifiedLang(Param):
    name: ParamName = ParamName.VERIFIED_LANG
    description: str = 'Verified language'
    value: bool = field(init=False)
    available_range: List[bool] = field(init=False)
    recommended_range: List[bool] = field(init=False)
    help_msg: str = (
        'Only workers that completed extra test and verified their knowledge of your chosen language '
        'will be able to see your task'
    )

    def __init__(self, task_spec: spec.PreparedTaskSpec):
        lang = task_spec.lang
        self.available_range = [False, True]
        self.recommended_range = [True]

        if lang in Languages.VERIFIED_LANGUAGES_TO_SKILLS:
            self.value = True
        else:
            self.value = False
            self.disabled = True
            self.help_msg = self.help_msg + '. This option is not available for your chosen language'


@dataclass(init=False)
class Overlap(Param):
    name: ParamName = ParamName.OVERLAP
    description: str = 'Overlap'
    value: str = field(init=False)
    available_range: List[str] = field(init=False)
    recommended_range: List[str] = field(init=False)
    help_msg: str = (
        'Number of accepted solutions that will be collected for each individual task. '
        'Can be static - e.g. "3" or dynamic - e.g. "3-5". Custom overlap values are supported.'
    )

    @staticmethod
    def get_values(
        quality_preset: str,
        overlap: Optional[str] = None,
    ):
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            overlap = {
                QualityPresetType.NO_CONTROL.value: '1',
                QualityPresetType.MILD.value: '2-3',
                QualityPresetType.MODERATE.value: '3',
                QualityPresetType.STRICT.value: '3-5',
            }[quality_preset]

        else:
            if overlap is None:
                overlap = '3'

        return overlap, disabled

    def __init__(self, quality_preset: str):
        self.available_range = ['1', '2-3', '3', '3-5', '5']
        self.recommended_range = ['2-3', '3']

        self.value, self.disabled = self.get_values(quality_preset)

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            update = event.update[self.name]
            try:
                get_overlap(update.value, 0.5)
                return super().process(event) + [Event(sender=self.name.value, update={self.name: update})]
            except:
                return []

        quality_preset = event.update[ParamName.QUALITY_PRESET].value

        self.value, self.disabled = self.get_values(
            quality_preset,
            self.value,
        )
        return [Event(sender=self.name.value, update={self.name: Update(self.value, self.disabled)})]


class MaxAttempts(Overlap):
    description: str = 'Max attempts'

    help_msg: str = (
        'Maximum number of annotation attempts that will be collected for each individual task. '
        'Custom overlap values are supported.'
    )

    @staticmethod
    def get_values(
        quality_preset: str,
        max_attempts: Optional[str] = None,
    ):
        disabled = quality_preset != QualityPresetType.CUSTOM.value
        if disabled:
            max_attempts = {
                QualityPresetType.MILD.value: '2',
                QualityPresetType.MODERATE.value: '3',
                QualityPresetType.STRICT.value: '5',
            }[quality_preset]

        else:
            if max_attempts is None:
                max_attempts = '3'

        return max_attempts, disabled

    def __init__(self, quality_preset: str):
        self.available_range = ['2', '3', '5', '7']
        self.recommended_range = ['2', '3']

        self.value, self.disabled = self.get_values(quality_preset)

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            update = event.update[self.name]
            try:
                assert int(update.value) > 0
                return super().process(event) + [Event(sender=self.name.value, update={self.name: update})]
            except:
                return []

        quality_preset = event.update[ParamName.QUALITY_PRESET].value

        self.value, self.disabled = self.get_values(
            quality_preset,
            self.value,
        )
        return [Event(sender=self.name.value, update={self.name: Update(self.value, self.disabled)})]


@dataclass(init=False)
class OverlapConfidence(Param):
    name: ParamName = ParamName.OVERLAP_CONFIDENCE
    description: str = 'Dynamic overlap confidence'
    value: float = 0.85
    available_range: List[float] = field(init=False)
    recommended_range: List[float] = field(init=False)
    help_msg: str = (
        'Applicable for dynamic overlap only. Threshold confidence of solution to qualify as "good", '
        'which can happen before number of attempts reaches maximum'
    )

    def __init__(self, quality_preset: str, overlap: str):
        self.disabled = quality_preset != QualityPresetType.CUSTOM.value or overlap_is_static(overlap)
        self.available_range = [0.0, 1.0]
        self.recommended_range = [0.75, 0.90]

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        overlap_update = event.update[ParamName.OVERLAP]
        disabled = overlap_update.disabled or overlap_is_static(overlap_update.value)

        if disabled:
            self.value, self.disabled = 0.85, True
        else:
            self.disabled = False
        return []


class Confidence(OverlapConfidence):
    description: str = 'Annotation confidence'
    help_msg: str = (
        'Threshold confidence of annotation to qualify as "good", '
        'which can happen before number of attempts reaches maximum'
    )

    def process(self, event: Event) -> List[Event]:
        if isinstance(event, UIEvent):
            return super().process(event)

        overlap_update = event.update[ParamName.OVERLAP]
        disabled = overlap_update.disabled

        if disabled:
            self.value, self.disabled = 0.85, True
        else:
            self.disabled = False
        return []


@dataclass(init=False)
class AggregationAlgorithm(Param):
    name: ParamName = ParamName.AGGREGATION_ALGORITHM
    description: str = 'Aggregation'
    value: str = 'DS'
    available_range: List[str] = field(init=False)
    recommended_range: List[str] = field(init=False)
    help_msg: str = (
        'Aggregation algorithm that will be applied to solutions. '
        'Options are: Majority Vote, '
        '<a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.469.1377&rep=rep1&type=pdf"> Dawid-Skene</a>'
        ' and Max Likelihood'
    )

    def __init__(self):
        self.available_range = ['MV', 'ML', 'DS']
        self.recommended_range = ['ML', 'DS']

from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Dict, Deque


import toloka.client as toloka

from .. import params as params_module, classification_loop, control, evaluation, pricing, task_spec as spec, worker
from ..pricing import _get_num_classes

from . import params
from .base import Event, Update, UIEvent, Node, ParamName
from .characteristics import _get_stats_and_chars, _get_annotation_stats_and_chars
from .client import Params, AnnotationParams
from .params import Param, Characteristics, Stat
from .util import get_overlap, get_agg_algorithm


@dataclass
class EventLoop:
    event_to_subscribers: Dict[str, Node]
    params: List[Node]
    stats: List[Stat]
    characteristics: Characteristics
    state: Dict[ParamName, Param] = field(init=False)
    queue: Deque[Event] = field(default_factory=deque)
    ui_initialized: bool = False

    def __post_init__(self):
        self.state = {p.name: p for p in self.params if isinstance(p, Param)}
        self.update_stats_and_chars()

    def update_stats_and_chars(self):
        stats, characteristics = _get_stats_and_chars(
            num_classes=self.state[ParamName.CONTROL_TASKS_FOR_ACCEPT].num_classes,
            **{
                name.value: self.state[name].value
                for name in [
                    ParamName.TASK_DURATION_HINT,
                    ParamName.TASK_COUNT,
                    ParamName.CONTROL_TASK_COUNT,
                    ParamName.ASSIGNMENT_PRICE,
                    ParamName.OVERLAP,
                    ParamName.OVERLAP_CONFIDENCE,
                    ParamName.CONTROL_TASKS_FOR_ACCEPT,
                    ParamName.VERIFIED_LANG,
                    ParamName.TRAINING,
                    ParamName.TRAINING_SCORE,
                ]
            },
        )
        for v, s in zip(stats, self.stats):
            s.process(Event(ParamName.STATISTICS.value, {s.name: Update(v)}))

        self.characteristics.process(
            Event(ParamName.CHARACTERISTICS.value, {self.characteristics.name: Update(characteristics)})
        )

    def start(self, event: UIEvent):
        if not self.ui_initialized:
            return

        changed = False
        if event.sender == ParamName.SPEED_CONTROL.ui:
            changed = self.state[ParamName.SPEED_CONTROL].check_update(event)
        else:
            for name, update in event.update.items():
                p = self.state[name]
                old = Update(p.value, p.disabled)
                if old == update:
                    continue
                changed = True
                break
        if not changed:
            # redundant event from dispatch
            return

        self.queue.append(event)

        while self.queue:
            e = self.queue.popleft()
            for p in self.event_to_subscribers.get(e.sender, []):
                self.queue.extend(p.process(e))

        self.update_stats_and_chars()

    def get_params(self) -> Params:
        # todo: proxy age_restriction to pool builder
        task_duration_hint = self.state[ParamName.TASK_DURATION_HINT].value
        control_task_count = self.state[ParamName.CONTROL_TASK_COUNT].value

        correct_control_task_ratio_for_acceptance = (
            0.0
            if control_task_count == 0
            else self.state[ParamName.CONTROL_TASKS_FOR_ACCEPT].value / control_task_count
        )
        builder = (
            control.RuleBuilder()
            .add_static_reward(threshold=correct_control_task_ratio_for_acceptance)
            .add_complex_speed_control(self.state[ParamName.SPEED_CONTROL].value)
        )

        if control_task_count > 0:
            builder = builder.add_control_task_control(
                control_task_count=control_task_count,
                control_task_correct_count_for_hard_block=0,
                control_task_correct_count_for_soft_block=self.state[ParamName.CONTROL_TASKS_FOR_BLOCK].value,
            )

        training_score = None
        if self.state[ParamName.TRAINING].value:
            training_score = self.state[ParamName.TRAINING_SCORE].value
        return params_module.Params(
            worker_filter=worker.WorkerFilter(
                filters=[
                    worker.WorkerFilter.Params(
                        langs={
                            worker.LanguageRequirement(
                                lang=self.state[ParamName.LANGUAGE].value,
                                verified=self.state[ParamName.VERIFIED_LANG].value,
                            )
                        },
                        regions=worker.lang_to_default_regions.get(self.state[ParamName.LANGUAGE].value, {}),
                    )
                ],
                training_score=training_score,
            ),
            task_duration_hint=task_duration_hint,
            pricing_config=pricing.PoolPricingConfig(
                real_tasks_count=self.state[ParamName.TASK_COUNT].value,
                control_tasks_count=control_task_count,
                assignment_price=self.state[ParamName.ASSIGNMENT_PRICE].value,
            ),
            overlap=get_overlap(self.state[ParamName.OVERLAP].value, self.state[ParamName.OVERLAP_CONFIDENCE].value),
            aggregation_algorithm=get_agg_algorithm(self.state[ParamName.AGGREGATION_ALGORITHM].value),
            control=control.Control(
                rules=builder.build(),
            ),
            task_duration_function=None,
        )


class AnnotationEventLoop(EventLoop):
    def update_stats_and_chars(self):
        stats, characteristics = _get_annotation_stats_and_chars(
            **{
                name.value: self.state[name].value
                for name in [
                    ParamName.TASK_DURATION_HINT,
                    ParamName.TASK_COUNT,
                    ParamName.ASSIGNMENT_PRICE,
                    ParamName.CHECK_SAMPLE,
                    ParamName.MAX_TASKS_TO_CHECK,
                    ParamName.ACCURACY_THRESHOLD,
                    ParamName.OVERLAP,
                    ParamName.OVERLAP_CONFIDENCE,
                    ParamName.CONTROL_TASKS_FOR_ACCEPT,
                    ParamName.VERIFIED_LANG,
                ]
            },
        )
        for v, s in zip(stats, self.stats):
            s.process(Event(ParamName.STATISTICS.value, {s.name: Update(v)}))

        self.characteristics.process(
            Event(ParamName.CHARACTERISTICS.value, {self.characteristics.name: Update(characteristics)})
        )

    def get_params(self) -> AnnotationParams:
        # todo: age_restriction to pool builder
        task_duration_hint = self.state[ParamName.TASK_DURATION_HINT].value
        checked_task_count = self.state[ParamName.TASK_COUNT].value
        assignment_check_sample = None

        if self.state[ParamName.CHECK_SAMPLE].value:
            checked_task_count = self.state[ParamName.MAX_TASKS_TO_CHECK].value
            assignment_accuracy_finalization_threshold = (
                0.0 if checked_task_count == 0 else self.state[ParamName.ACCURACY_THRESHOLD].value / checked_task_count
            )
            assignment_check_sample = evaluation.AssignmentCheckSample(
                max_tasks_to_check=checked_task_count,
                assignment_accuracy_finalization_threshold=assignment_accuracy_finalization_threshold,
            )
        correct_checked_task_ratio_for_acceptance = (
            0.0
            if checked_task_count == 0
            else self.state[ParamName.CONTROL_TASKS_FOR_ACCEPT].value / checked_task_count
        )
        builder = (
            control.RuleBuilder()
            .add_static_reward(threshold=correct_checked_task_ratio_for_acceptance)
            .add_complex_speed_control(self.state[ParamName.SPEED_CONTROL].value)
        )

        training_score = None
        return AnnotationParams(
            worker_filter=worker.WorkerFilter(
                filters=[
                    worker.WorkerFilter.Params(
                        langs={
                            worker.LanguageRequirement(
                                lang=self.state[ParamName.LANGUAGE].value,
                                verified=self.state[ParamName.VERIFIED_LANG].value,
                            )
                        },
                        regions=worker.lang_to_default_regions.get(self.state[ParamName.LANGUAGE].value, {}),
                    )
                ],
                training_score=training_score,
            ),
            task_duration_hint=task_duration_hint,
            pricing_config=pricing.PoolPricingConfig(
                real_tasks_count=self.state[ParamName.TASK_COUNT].value,
                control_tasks_count=0,
                assignment_price=self.state[ParamName.ASSIGNMENT_PRICE].value,
            ),
            overlap=classification_loop.DynamicOverlap(
                min_overlap=1,
                max_overlap=int(self.state[ParamName.OVERLAP].value),
                confidence=self.state[ParamName.OVERLAP_CONFIDENCE].value,
            ),
            aggregation_algorithm=None,
            control=control.Control(
                rules=builder.build(),
            ),
            assignment_check_sample=assignment_check_sample,
            task_duration_function=None,
        )


def get_loop(
    task_spec: spec.PreparedTaskSpec,
    task_duration_hint: timedelta,
    toloka_client: toloka.TolokaClient,
) -> EventLoop:
    quality = params.Stat(ParamName.QUALITY, 'Quality')
    time_ = params.Stat(ParamName.TIME, 'Time', reverse=True)
    cost = params.Stat(ParamName.COST, 'Cost', reverse=True)

    help_p = params.Help()
    task_duration_p = params.TaskDuration(task_duration_hint)
    lang_p = params.Lang(task_spec)
    training_p = params.Training(task_spec, toloka_client)
    task_count_p = params.TaskCount(task_duration_hint)
    quality_preset_p = params.QualityPreset()
    control_task_count_p = params.ControlTaskCount(task_count_p.value, quality_preset_p.value)
    assignment_price_p = params.AssignmentPrice(task_duration_hint, task_count_p.value, control_task_count_p.value)
    age_restriction_p = params.AgeRestriction()
    verified_lang_p = params.VerifiedLang(task_spec)
    training_value_p = params.TrainingValue(training_p.value)
    overlap_p = params.Overlap(quality_preset_p.value)
    overlap_confidence_p = params.OverlapConfidence(quality_preset_p.value, overlap_p.value)
    agg_p = params.AggregationAlgorithm()
    control_tasks_for_accept_p = params.ControlTasksForAccept(
        _get_num_classes(task_spec.task_mapping),
        quality_preset_p.value,
        control_task_count_p.value,
    )
    control_tasks_for_block_p = params.ControlTasksForBlock(
        quality_preset_p.value,
        control_task_count_p.value,
    )
    speed_control_p = params.SpeedControlList(quality_preset_p.value)

    general_quality_group = params.ParamGroup(
        name=ParamName.GENERAL_QUALITY_GROUP,
        params=[task_duration_p, lang_p, quality_preset_p],
    )

    quality_tasks_group = params.ParamGroup(
        name=ParamName.QUALITY_TASKS_GROUP,
        params=[task_duration_p, quality_preset_p, task_count_p],
    )

    quality_assignment_group = params.ParamGroupControlTasksRecalculate(
        name=ParamName.QUALITY_ASSIGNMENT_GROUP,
        params=[task_duration_p, quality_preset_p, task_count_p, control_task_count_p],
    )
    created_params = [
        help_p,
        task_duration_p,
        lang_p,
        task_count_p,
        control_task_count_p,
        control_tasks_for_accept_p,
        control_tasks_for_block_p,
        quality_preset_p,
        overlap_p,
        overlap_confidence_p,
        agg_p,
        assignment_price_p,
        age_restriction_p,
        verified_lang_p,
        training_p,
        training_value_p,
        speed_control_p,
        general_quality_group,
        quality_tasks_group,
        quality_assignment_group,
    ]
    stats = [quality, time_, cost]
    characteristics = params.Characteristics()
    return EventLoop(
        event_to_subscribers={
            ParamName.QUALITY_PRESET.ui: [general_quality_group],
            ParamName.GENERAL_QUALITY_GROUP.value: [quality_tasks_group, overlap_p, speed_control_p],
            ParamName.TASK_COUNT.ui: [quality_tasks_group],
            ParamName.QUALITY_TASKS_GROUP.value: [quality_assignment_group],
            ParamName.CONTROL_TASK_COUNT.ui: [quality_assignment_group],
            ParamName.QUALITY_ASSIGNMENT_GROUP.value: [
                task_count_p,
                control_task_count_p,
                assignment_price_p,
                quality_preset_p,
                control_tasks_for_accept_p,
                control_tasks_for_block_p,
            ],
            ParamName.OVERLAP.ui: [overlap_p],
            ParamName.OVERLAP.value: [overlap_confidence_p],
            ParamName.OVERLAP_CONFIDENCE.ui: [overlap_confidence_p],
            ParamName.ASSIGNMENT_PRICE.ui: [assignment_price_p],
            ParamName.CONTROL_TASKS_FOR_ACCEPT.ui: [control_tasks_for_accept_p],
            ParamName.CONTROL_TASKS_FOR_BLOCK.ui: [control_tasks_for_block_p],
            ParamName.AGGREGATION_ALGORITHM.ui: [agg_p],
            ParamName.TRAINING.ui: [training_p],
            ParamName.TRAINING.value: [training_value_p],
            ParamName.TRAINING_SCORE.ui: [training_value_p],
            ParamName.AGE_RESTRICTION.ui: [age_restriction_p],
            ParamName.VERIFIED_LANG.ui: [verified_lang_p],
            ParamName.HELP.value: [help_p],
            ParamName.SPEED_CONTROL.ui: [speed_control_p],
        },
        params=created_params,
        stats=stats,
        characteristics=characteristics,
    )


def get_loop_for_annotation(
    task_spec: spec.PreparedTaskSpec,
    task_duration_hint: timedelta,
    toloka_client: toloka.TolokaClient,
) -> AnnotationEventLoop:
    quality = params.Stat(ParamName.QUALITY, 'Quality')
    time_ = params.Stat(ParamName.TIME, 'Time', reverse=True)
    cost = params.Stat(ParamName.COST, 'Cost', reverse=True)

    help_p = params.Help()
    task_duration_p = params.TaskDuration(task_duration_hint)
    lang_p = params.Lang(task_spec)
    task_count_p = params.TaskCount(task_duration_hint)
    quality_preset_p = params.AnnotationQualityPreset()
    assignment_price_p = params.AnnotationAssignmentPrice(task_duration_hint, task_count_p.value)
    age_restriction_p = params.AgeRestriction()
    verified_lang_p = params.VerifiedLang(task_spec)
    max_attempts_p = params.MaxAttempts(quality_preset_p.value)
    check_sample_p = params.CheckSample(quality_preset_p.value)
    max_tasks_to_check_p = params.MaxTasksToCheck(quality_preset_p.value, task_count_p.value, check_sample_p.value)
    accuracy_threshold_p = params.AccuracyThreshold(
        quality_preset_p.value, task_count_p.value, check_sample_p.value, max_tasks_to_check_p.value
    )
    overlap_confidence_p = params.Confidence(quality_preset_p.value, max_attempts_p.value)
    control_tasks_for_accept_p = params.CheckedTasksForAccept(
        quality_preset_p.value,
        task_count_p.value,
    )
    control_tasks_for_block_p = params.CheckedTasksForBlock(
        quality_preset_p.value,
        task_count_p.value,
    )
    speed_control_p = params.SpeedControlList(quality_preset_p.value)

    general_quality_group = params.ParamGroup(
        name=ParamName.GENERAL_QUALITY_GROUP,
        params=[task_duration_p, lang_p, quality_preset_p],
    )

    quality_tasks_group = params.ParamGroup(
        name=ParamName.QUALITY_TASKS_GROUP,
        params=[task_duration_p, quality_preset_p, task_count_p],
    )

    quality_assignment_group = params.ParamGroup(
        name=ParamName.QUALITY_ASSIGNMENT_GROUP,
        params=[task_duration_p, quality_preset_p, task_count_p, check_sample_p],
    )

    check_sample_group = params.ParamGroupCheckedTasksRecalculate(
        name=ParamName.CHECK_SAMPLE_GROUP,
        params=[quality_preset_p, task_count_p, check_sample_p, max_tasks_to_check_p],
    )
    created_params = [
        help_p,
        task_duration_p,
        lang_p,
        task_count_p,
        check_sample_p,
        max_tasks_to_check_p,
        accuracy_threshold_p,
        control_tasks_for_accept_p,
        control_tasks_for_block_p,
        quality_preset_p,
        max_attempts_p,
        overlap_confidence_p,
        assignment_price_p,
        age_restriction_p,
        verified_lang_p,
        speed_control_p,
        general_quality_group,
        quality_tasks_group,
        quality_assignment_group,
        check_sample_group,
    ]
    stats = [quality, time_, cost]
    characteristics = params.Characteristics()
    return AnnotationEventLoop(
        event_to_subscribers={
            ParamName.QUALITY_PRESET.ui: [general_quality_group],
            ParamName.GENERAL_QUALITY_GROUP.value: [quality_tasks_group, max_attempts_p, speed_control_p],
            ParamName.TASK_COUNT.ui: [quality_tasks_group],
            ParamName.QUALITY_TASKS_GROUP.value: [quality_assignment_group],
            ParamName.CHECK_SAMPLE.ui: [quality_assignment_group],
            ParamName.MAX_TASKS_TO_CHECK.ui: [check_sample_group],
            ParamName.QUALITY_ASSIGNMENT_GROUP.value: [
                check_sample_group,
                assignment_price_p,
            ],
            ParamName.CHECK_SAMPLE_GROUP.value: [
                task_count_p,
                check_sample_p,
                quality_preset_p,
                max_tasks_to_check_p,
                accuracy_threshold_p,
                control_tasks_for_accept_p,
                control_tasks_for_block_p,
            ],
            ParamName.OVERLAP.ui: [max_attempts_p],
            ParamName.OVERLAP.value: [overlap_confidence_p],
            ParamName.OVERLAP_CONFIDENCE.ui: [overlap_confidence_p],
            ParamName.ASSIGNMENT_PRICE.ui: [assignment_price_p],
            ParamName.CONTROL_TASKS_FOR_ACCEPT.ui: [control_tasks_for_accept_p],
            ParamName.CONTROL_TASKS_FOR_BLOCK.ui: [control_tasks_for_block_p],
            ParamName.ACCURACY_THRESHOLD.ui: [accuracy_threshold_p],
            ParamName.AGE_RESTRICTION.ui: [age_restriction_p],
            ParamName.VERIFIED_LANG.ui: [verified_lang_p],
            ParamName.HELP.value: [help_p],
            ParamName.SPEED_CONTROL.ui: [speed_control_p],
        },
        params=created_params,
        stats=stats,
        characteristics=characteristics,
    )

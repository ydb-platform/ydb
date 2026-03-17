import abc
import math
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from functools import lru_cache
from math import ceil
import numpy as np
from plotly.graph_objs import Figure
import plotly.graph_objs as go
from typing import List, Optional, Tuple, Union

import toloka.client as toloka

from . import base, mapping, classification_loop, evaluation


@dataclass
class PricingStrategy(abc.ABC):
    ...


@dataclass
class StaticPricingStrategy(PricingStrategy):
    ...


@dataclass
class DynamicPricingStrategy(PricingStrategy):
    max_ratio: float  # max price for "good" work divided by price for "ok" work


@dataclass
class PoolPricingConfig:
    assignment_price: float
    real_tasks_count: int
    control_tasks_count: int

    def get_tasks_count(self) -> int:
        return self.real_tasks_count + self.control_tasks_count


@dataclass
class TrainingConfig:
    training_tasks_in_task_suite_count: int
    task_suites_required_to_pass: int
    max_assignment_duration: timedelta

    def get_task_duration_hint(self) -> timedelta:
        return self.max_assignment_duration / self.training_tasks_in_task_suite_count / training_overhead_multiplier

    @staticmethod
    def from_toloka_training(
        training: toloka.Training,
        task_duration_hint: Optional[timedelta] = None,
        available_task_count: Optional[int] = None,
    ) -> 'TrainingConfig':
        if task_duration_hint is None:
            return TrainingConfig(
                training.training_tasks_in_task_suite_count,
                training.task_suites_required_to_pass,
                timedelta(seconds=training.assignment_max_duration_seconds),
            )
        assert available_task_count is not None
        training_time = (
            timedelta(seconds=training.assignment_max_duration_seconds)
            * training.task_suites_required_to_pass
            / training_overhead_multiplier
        )
        return choose_default_training_option(
            get_training_options(task_duration_hint, available_task_count, training_time)
        )


toloka_avg_price_per_hour = {'EN': 1.0, 'RU': 0.6, 'TR': 1.2, 'KK': 0.8}

toloka_commission = 0.3
min_toloka_commission = 0.001

min_assignment_cost = 0.005
threshold_for_assignment_cost_granularity = 1.0

min_assignment_duration = timedelta(seconds=30)
max_assignment_duration = timedelta(minutes=20)

min_tasks_in_assignment = 1
max_tasks_in_assignment = 30

min_training_assignments_to_pass = 1
max_training_assignments_to_pass = 5

min_training_tasks_to_pass = 2
max_training_tasks_to_pass = 30

assert max_tasks_in_assignment >= min_tasks_in_assignment
assert max_training_assignments_to_pass >= min_training_assignments_to_pass
assert max_training_tasks_to_pass >= min_training_tasks_to_pass

assert min_training_tasks_to_pass >= min_tasks_in_assignment * min_training_assignments_to_pass
assert max_training_tasks_to_pass <= max_tasks_in_assignment * max_training_assignments_to_pass

default_training_time = timedelta(minutes=3)

# configures extra time to account for not accustomed to the task workers
# do not change it between launches with same data, because internal calculations depend on it
training_overhead_multiplier = 10


@lru_cache(maxsize=None)
def _choose(n: int, k: int) -> int:
    result = 1
    for j in range(k):
        result *= n - j
        result //= j + 1
    return result


@lru_cache(maxsize=None)
def _get_random_chance(
    control_task_count: int,
    correct_control_task_count_for_acceptance: int,
    num_classes: int,
) -> float:
    sum_probability = 0.0
    for applicable_control_task_count in range(correct_control_task_count_for_acceptance, control_task_count + 1):
        multiplier = _choose(control_task_count, applicable_control_task_count)
        random_chance = (
            (1 / num_classes) ** applicable_control_task_count
            * (1 - 1 / num_classes) ** (control_task_count - applicable_control_task_count)
            * multiplier
        )
        sum_probability += random_chance
    return min(1.0, max(0.0, sum_probability))


def _get_num_classes(task_mapping: mapping.TaskMapping) -> int:
    return len(task_mapping.output_mapping[0].obj_meta.type.possible_values())


@dataclass
class PoolPricingConfigWithCalculatedProperties:
    config: PoolPricingConfig
    price_per_hour: float
    robustness: float
    control_tasks_ratio: float
    assignment_duration: timedelta


def _produce_pricing_config_with_calculated_properties(
    task_duration_hint: timedelta,
    random_chance: float,
    config: PoolPricingConfig,
) -> PoolPricingConfigWithCalculatedProperties:
    tasks_count = config.get_tasks_count()
    assignment_per_hour = timedelta(hours=1).total_seconds() / (task_duration_hint * tasks_count).total_seconds()
    return PoolPricingConfigWithCalculatedProperties(
        config=config,
        price_per_hour=config.assignment_price * assignment_per_hour,
        robustness=1 - random_chance,
        control_tasks_ratio=config.control_tasks_count / tasks_count,
        assignment_duration=task_duration_hint * tasks_count,
    )


def _readable_duration(duration: timedelta) -> str:
    # round up to seconds
    return str(timedelta(seconds=ceil(duration.total_seconds())))


def _get_price_per_hour(lang: str) -> float:
    return toloka_avg_price_per_hour.get(lang, toloka_avg_price_per_hour[base.DEFAULT_LANG])


def get_control_tasks_for_accept_from_ratio(ratio: float, control_tasks_count: int) -> int:
    return ceil(ratio * control_tasks_count)


def calculate_properties_for_pricing_config(
    config: PoolPricingConfig,
    task_duration_hint: timedelta,
    correct_control_task_ratio_for_acceptance: float,
    task_mapping: mapping.TaskMapping,
) -> PoolPricingConfigWithCalculatedProperties:
    num_classes = _get_num_classes(task_mapping)
    random_chance = _get_random_chance(
        config.control_tasks_count,
        get_control_tasks_for_accept_from_ratio(correct_control_task_ratio_for_acceptance, config.control_tasks_count),
        num_classes,
    )
    return _produce_pricing_config_with_calculated_properties(task_duration_hint, random_chance, config)


def round_price(value: float) -> float:
    # need for round:
    # 1. to remove displays such as '0.0299999999999' instead of '0.3', which can be got from float price calculations
    # 2. we have different granularity for prices lower and higher than 1$ threshold
    digits = 2 if value > threshold_for_assignment_cost_granularity else 3
    return round(value, digits)


def _get_assignment_params(
    task_count: int,
    task_duration_hint: timedelta,
    price_per_hour: Optional[float],
    fixed_price: Optional[float] = None,
) -> Tuple[timedelta, float, float]:
    price_per_second = price_per_hour / (60 * 60) if price_per_hour is not None else None

    duration = task_count * task_duration_hint

    price = max(
        min_assignment_cost,
        round_price(duration.total_seconds() * price_per_second) if fixed_price is None else fixed_price,
    )
    commission = toloka_commission * price

    return duration, price, commission


def get_pricing_options(
    task_duration_hint: timedelta,
    pricing_strategy: PricingStrategy,
    lang: str,
    task_mapping: mapping.TaskMapping,
    max_random_chance_for_control_tasks: float = 1 / 3,
    max_control_task_percentage_on_page: float = 1 / 3,
    correct_control_task_ratio_for_acceptance: float = 1.0,
    plot: bool = False,
) -> List[PoolPricingConfigWithCalculatedProperties]:
    assert len(task_mapping.output_mapping) == 1
    assert issubclass(task_mapping.output_mapping[0].obj_meta.type, base.Label)
    num_classes = _get_num_classes(task_mapping)

    if not isinstance(task_duration_hint, timedelta):
        raise NotImplementedError(task_duration_hint)

    if not isinstance(pricing_strategy, StaticPricingStrategy):
        raise NotImplementedError(pricing_strategy)

    suitable_options = []

    # min_control_task_count = ceil(log(max_random_chance_for_control_tasks) / log(1 / num_classes))

    price_per_hour = _get_price_per_hour(lang)

    for task_count in range(min_tasks_in_assignment, max_tasks_in_assignment):
        assignment_duration, assignment_price, assignment_commission = _get_assignment_params(
            task_count, task_duration_hint, price_per_hour
        )

        for control_task_count in range(1, int(ceil(max_control_task_percentage_on_page * task_count)) + 1):
            random_chance = _get_random_chance(
                control_task_count,
                get_control_tasks_for_accept_from_ratio(correct_control_task_ratio_for_acceptance, control_task_count),
                num_classes,
            )
            suitable = (
                random_chance <= max_random_chance_for_control_tasks
                and min_assignment_duration <= assignment_duration <= max_assignment_duration
                and assignment_commission >= min_toloka_commission
            )

            if suitable:
                suitable_options.append(
                    _produce_pricing_config_with_calculated_properties(
                        task_duration_hint,
                        random_chance,
                        PoolPricingConfig(
                            real_tasks_count=task_count - control_task_count,
                            control_tasks_count=control_task_count,
                            assignment_price=assignment_price,
                        ),
                    )
                )

    if plot:
        plot_options(
            options=suitable_options,
            task_duration_hint=task_duration_hint,
            max_random_chance_for_control_tasks=max_random_chance_for_control_tasks,
            max_control_task_percentage_on_page=max_control_task_percentage_on_page,
            correct_control_task_ratio_for_acceptance=correct_control_task_ratio_for_acceptance,
            default_option=choose_default_option(suitable_options),
            num_classes=num_classes,
            lang=lang,
            price_per_hour=price_per_hour,
            html_path='pricing.html',
        )

    return suitable_options


def get_annotation_pricing_options(
    task_duration_hint: timedelta,
    pricing_strategy: PricingStrategy,
    lang: str,
    task_mapping: mapping.TaskMapping,
) -> List[PoolPricingConfig]:
    if not isinstance(pricing_strategy, StaticPricingStrategy):
        raise NotImplementedError(pricing_strategy)

    suitable_options = []

    price_per_hour = _get_price_per_hour(lang)

    for task_count in range(min_tasks_in_assignment, max_tasks_in_assignment):
        assignment_duration, assignment_price, assignment_commission = _get_assignment_params(
            task_count, task_duration_hint, price_per_hour
        )

        suitable = (
            min_assignment_duration <= assignment_duration <= max_assignment_duration
            and assignment_commission >= min_toloka_commission
        )

        if suitable:
            suitable_options.append(
                PoolPricingConfig(real_tasks_count=task_count, control_tasks_count=0, assignment_price=assignment_price)
            )
    return suitable_options


def choose_default_annotation_option(options: List[PoolPricingConfig]) -> PoolPricingConfig:
    assert options, 'No available options'
    return options[len(options) // 2]


def get_expert_pricing_options(
    task_duration_hint: timedelta,
    task_mapping: mapping.TaskMapping,  # todo we may use this in the future
    avg_price_per_hour: Optional[float] = None,
) -> List[PoolPricingConfig]:
    suitable_options = []

    minimal_pay_for_inhouse = avg_price_per_hour is None

    for task_count in range(min_tasks_in_assignment, max_tasks_in_assignment):
        assignment_duration, assignment_price, assignment_commission = _get_assignment_params(
            task_count,
            task_duration_hint,
            avg_price_per_hour,
            fixed_price=min_assignment_cost if minimal_pay_for_inhouse else None,
        )

        suitable = min_assignment_duration <= assignment_duration <= max_assignment_duration and (
            minimal_pay_for_inhouse or assignment_commission >= min_toloka_commission
        )

        if suitable:
            suitable_options.append(
                PoolPricingConfig(real_tasks_count=task_count, control_tasks_count=0, assignment_price=assignment_price)
            )

    return suitable_options


def get_training_options(
    task_duration_hint: timedelta,
    available_task_count: int,
    training_time: timedelta = default_training_time,
) -> List[TrainingConfig]:
    task_count = ceil(training_time / task_duration_hint)
    assert min_training_tasks_to_pass <= task_count <= max_training_tasks_to_pass, (
        'You are trying to create either too short or too long training. Expected from '
        f'{min_training_tasks_to_pass} to {max_training_tasks_to_pass} tasks, got {task_count}'
    )
    assert task_count <= available_task_count, (
        'You do not have enough tasks for this training. Expected at least '
        f'{task_count} tasks for this parameters, got {available_task_count}'
    )
    configs = []
    for assignments_count in range(min_training_assignments_to_pass, max_training_assignments_to_pass + 1):
        tasks_in_assignment = ceil(task_count / assignments_count)
        if min_tasks_in_assignment <= tasks_in_assignment <= max_tasks_in_assignment:
            config = TrainingConfig(
                training_tasks_in_task_suite_count=tasks_in_assignment,
                task_suites_required_to_pass=assignments_count,
                max_assignment_duration=tasks_in_assignment * task_duration_hint * training_overhead_multiplier,
            )
            configs.append(config)

    return configs


def choose_default_training_option(options: List[TrainingConfig]) -> TrainingConfig:
    assert options, 'No available options'
    return options[len(options) // 2]


def choose_default_option(
    suitable_options: List[PoolPricingConfigWithCalculatedProperties],
) -> PoolPricingConfigWithCalculatedProperties:
    assert suitable_options, 'No suitable options found'

    filtered_options = list(suitable_options)

    # choose option by average over chosen dimensions
    for value_f in (
        lambda choice: choice.robustness,
        lambda choice: choice.config.control_tasks_count,
        lambda choice: choice.config.control_tasks_count + choice.config.real_tasks_count,
    ):
        values = np.array([value_f(option) for option in filtered_options])
        avg_value = values[np.abs(values - values.mean()).argmin()]
        filtered_options = [option for option in filtered_options if value_f(option) == avg_value]

    return filtered_options[0]  # there can be multiple options with same last filter value


def choose_default_expert_option(
    suitable_options: List[PoolPricingConfig],
    avg_price_per_hour: Optional[float],
) -> PoolPricingConfig:
    assert suitable_options, 'No suitable options found'
    return (min if avg_price_per_hour is not None else max)(
        suitable_options, key=lambda config: config.get_tasks_count()
    )


def _get_plots_arguments(
    options: List[PoolPricingConfigWithCalculatedProperties],
) -> Tuple[List[float], List[int], List[float], List[int], List[List[Union[float, int]]]]:
    x, y, r, t, texts = [], [], [], [], []

    for option in options:
        tasks_count = option.config.get_tasks_count()
        task_price_cents = (option.config.assignment_price * 100) / option.config.real_tasks_count

        x.append(option.robustness)
        y.append(int(option.assignment_duration.total_seconds()))
        r.append(option.control_tasks_ratio)
        t.append(tasks_count)

        texts.append(
            [
                task_price_cents,
                option.config.assignment_price,
                option.robustness,
                option.control_tasks_ratio,
                option.config.real_tasks_count,
                option.config.control_tasks_count,
                _readable_duration(option.assignment_duration),
            ]
        )
    return x, y, r, t, texts


TEMPLATE = (
    'Price per task, ¢: %{customdata[0]:.2f}<br>'
    'Price per assignment, $: %{customdata[1]:.2f}<br>'
    'Robustness: %{customdata[2]:.2f}<br>'
    'Control task ratio: %{customdata[3]:.2f}<br>'
    'Real task count: %{customdata[4]}<br>'
    'Control task count: %{customdata[5]}<br>'
    'Assignment duration: %{customdata[6]}<extra></extra>'
)


# todo marker & font size


class PlotDimensions(Enum):
    DURATION = 'Assignment duration (seconds)'
    ROBUSTNESS = 'Random robustness'
    CONTROL_TASK_RATIO = 'Control task ratio'


class AvailableConfigurations(Enum):
    ROBUSTNESS_AND_DURATION = (PlotDimensions.ROBUSTNESS, PlotDimensions.DURATION, PlotDimensions.CONTROL_TASK_RATIO)
    CONTROL_AND_DURATION = (PlotDimensions.CONTROL_TASK_RATIO, PlotDimensions.DURATION, PlotDimensions.ROBUSTNESS)
    ROBUSTNESS_AND_CONTROL = (PlotDimensions.ROBUSTNESS, PlotDimensions.CONTROL_TASK_RATIO, PlotDimensions.DURATION)


@dataclass
class PlotConfiguration:
    dimensions: AvailableConfigurations
    coloraxis: str = field(init=False)
    x_index: int = field(init=False)
    y_index: int = field(init=False)
    c_index: int = field(init=False)
    reversescale: bool = field(init=False)
    need_x_adjustment: bool = field(init=False)
    need_y_adjustment: bool = field(init=False)

    def __post_init__(self):
        if self.dimensions == AvailableConfigurations.ROBUSTNESS_AND_DURATION:
            x, y, c, axis = 0, 1, 2, 1
        elif self.dimensions == AvailableConfigurations.CONTROL_AND_DURATION:
            x, y, c, axis = 2, 1, 0, 2
        elif self.dimensions == AvailableConfigurations.ROBUSTNESS_AND_CONTROL:
            x, y, c, axis = 0, 2, 1, 3
        else:
            raise ValueError(f'unsupported dimensions: {self.dimensions}')
        self.x_index, self.y_index, self.c_index = x, y, c
        self.coloraxis = f'coloraxis{axis}'
        self.reversescale = self.dimensions != AvailableConfigurations.CONTROL_AND_DURATION
        self.need_x_adjustment = self.dimensions != AvailableConfigurations.CONTROL_AND_DURATION
        self.need_y_adjustment = self.dimensions != AvailableConfigurations.ROBUSTNESS_AND_CONTROL


def _scatter_plot(
    configuration: PlotConfiguration,
    options: List[PoolPricingConfigWithCalculatedProperties],
    task_duration_hint: timedelta,
    max_random_chance_for_control_tasks: float,
    default_option: Optional[PoolPricingConfig] = None,
    row: Optional[int] = None,
    col: Optional[int] = None,
    html_path: Optional[str] = None,
    showfig: bool = True,
    fig: Figure = None,
):
    if default_option is None:
        default_option = choose_default_option(options)
    params = _get_plots_arguments(options)
    x, _, _, t, _ = params

    d_params = _get_plots_arguments([default_option])
    if fig is None:
        fig = go.Figure()

    plot_x, plot_y = params[configuration.x_index], params[configuration.y_index]
    plot_dx, plot_dy = d_params[configuration.x_index], d_params[configuration.y_index]
    color, dcolor = params[configuration.c_index], d_params[configuration.c_index]
    texts, dtexts = params[-1], d_params[-1]

    fig.add_trace(
        go.Scatter(
            x=plot_x,
            y=plot_y,
            customdata=texts,
            marker=dict(
                coloraxis=configuration.coloraxis, color=color, size=10, line=dict(width=1, color='DarkSlateGrey')
            ),
            mode='markers',
            name='choices',
            hovertemplate=TEMPLATE,
        ),
        row=row,
        col=col,
    )
    fig.add_trace(
        go.Scatter(
            x=plot_dx,
            y=plot_dy,
            customdata=dtexts,
            marker=dict(coloraxis=configuration.coloraxis, color=dcolor, size=12, line=dict(width=2, color='black')),
            mode='markers',
            name='Default choice',
            hovertemplate=f'<b>Recommended option</b><br>{TEMPLATE}',
        ),
        row=row,
        col=col,
    )

    if configuration.need_x_adjustment:
        fig.add_vline(
            x=1.0 - max_random_chance_for_control_tasks,
            line_dash='dashdot',
            annotation_text='min robustness',
            line_color='red',
            line_width=1,
            annotation=dict(textangle=90, y=0.7, font_color='darkred', font_size=8),
            row=row,
            col=col,
        )
        fig.update_xaxes(range=[1.0 - max_random_chance_for_control_tasks - 0.01, max(x) + 0.01], row=row, col=col)

    if configuration.need_y_adjustment:
        duration_thresholds = [
            (max_assignment_duration / 4, 'green', 'comfort'),
            (max_assignment_duration * 3 / 4, 'orange', 'inconvenient'),
            (None, 'red', 'critical'),
        ]

        for i in range(len(duration_thresholds)):
            y1, color, name = duration_thresholds[i]

            if i > 0:
                y0, _, _ = duration_thresholds[i - 1]
            else:
                y0 = min_assignment_duration

            if i == len(duration_thresholds) - 1:
                y1 = max_assignment_duration

            fig.add_hrect(
                y0=int(y0.total_seconds()),
                y1=int(y1.total_seconds()),
                fillcolor=color,
                opacity=0.1,
                annotation_position='bottom right',
                annotation_text=f'{name} duration',
                annotation=dict(font_color=f'dark{color}', font_size=8),
                row=row,
                col=col,
            )

        task_duration_hint_seconds = int(task_duration_hint.total_seconds())

        fig.update_yaxes(
            range=[min(t) * task_duration_hint_seconds - 50, max(t) * task_duration_hint_seconds + 50], row=row, col=col
        )

    if row is None and col is None:
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            legend=dict(x=0.5, xanchor='center'),
            legend_orientation='h',
            xaxis_title=configuration.dimensions.value[0].value,
            yaxis_title=configuration.dimensions.value[1].value,
            showlegend=False,
            **{
                configuration.coloraxis: dict(
                    colorscale='RdYlGn',
                    colorbar=dict(title=configuration.dimensions.value[2].value),
                    reversescale=configuration.reversescale,
                )
            },
        )

    if html_path is not None:
        fig.write_html(html_path)

    if showfig:
        fig.show()


def plot_options(
    options: List[PoolPricingConfigWithCalculatedProperties],
    task_duration_hint: timedelta,
    max_random_chance_for_control_tasks: float,
    correct_control_task_ratio_for_acceptance: float,
    num_classes: int,
    lang: str,
    price_per_hour: float,
    max_control_task_percentage_on_page: float,
    default_option: Optional[PoolPricingConfigWithCalculatedProperties] = None,
    html_path: Optional[str] = None,
    showfig: bool = True,
):
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2,
        cols=2,
        vertical_spacing=0.2,
        horizontal_spacing=0.2,
        subplot_titles=('', '', '', 'Params'),
    )

    configs = (
        (AvailableConfigurations.ROBUSTNESS_AND_DURATION, 1, 1),
        (AvailableConfigurations.CONTROL_AND_DURATION, 1, 2),
        (AvailableConfigurations.ROBUSTNESS_AND_CONTROL, 2, 1),
    )

    for config, row, col in configs:
        _scatter_plot(
            PlotConfiguration(config),
            options,
            task_duration_hint,
            max_random_chance_for_control_tasks,
            default_option,
            row=row,
            col=col,
            showfig=False,
            fig=fig,
        )

    params = (
        f'Task duration hint: {_readable_duration(task_duration_hint)}<br>'
        f'"{lang}" avg price per hour: {price_per_hour:.2f}$<br>'
        f'Number of classes: {num_classes}<br>'
        f'Control correct ratio for accept: {correct_control_task_ratio_for_acceptance:.2f}<br>'
        f'Max random chance: {max_random_chance_for_control_tasks:.2f}<br>'
        f'Max control task ratio: {max_control_task_percentage_on_page:.2f}<br>'
        f'Number of options: {len(options)}'
    )

    fig.add_annotation(
        text=params,
        align='left',
        showarrow=False,
        x=1.1,
        y=0.8,
        bordercolor='black',
        borderwidth=1,
        bgcolor='white',
        font_size=25,
        row=2,
        col=2,
    )

    fig.update_xaxes(showgrid=False, showticklabels=False, row=2, col=2)
    fig.update_yaxes(showgrid=False, showticklabels=False, row=2, col=2)

    fig.update_layout(
        legend=dict(x=0.5, xanchor='center'),
        legend_orientation='h',
        title='Pricing',
        title_x=0.5,
        coloraxis1=dict(
            colorscale='RdYlGn',
            colorbar=dict(title=PlotDimensions.CONTROL_TASK_RATIO.value, x=0.46, y=0.8, len=0.4),
            reversescale=True,
            showscale=True,
        ),
        coloraxis2=dict(
            colorscale='RdYlGn',
            colorbar=dict(title=PlotDimensions.ROBUSTNESS.value, x=1.05, y=0.8, len=0.4),
            showscale=True,
        ),
        coloraxis3=dict(
            colorscale='RdYlGn',
            colorbar=dict(title=PlotDimensions.DURATION.value, x=0.46, y=0.2, len=0.4),
            reversescale=True,
            showscale=True,
        ),
        showlegend=False,
        height=1000,
        width=1600,
    )

    for i, (config, row, col) in enumerate(configs):
        x_dim, y_dim, _ = config.value
        fig['layout'][f'xaxis{i + 1}']['title'] = x_dim.value
        fig['layout'][f'yaxis{i + 1}']['title'] = y_dim.value

    if html_path is not None:
        fig.write_html(html_path)

    if showfig:
        fig.show()


def get_pool_price(
    input_objects_count: int,
    config: PoolPricingConfig,
    overlap: classification_loop.Overlap,
    display_formula: bool = False,
) -> str:
    formula = PoolPriceFormula(input_objects_count, config, overlap)
    if display_formula:
        from IPython.display import display, Math

        print('clear formula, which does not account edge cases like min commission and incomplete assignments')
        display(Math(formula.clear_formula()))  # noqa
        print('\nmore precise formula, which accounts more edge cases')
        display(Math(formula.precise_formula()))  # noqa
    return formula.total_price_precise_raw_str


def get_annotation_price(
    input_objects_count: int,
    markup_config: PoolPricingConfig,
    check_config: PoolPricingConfig,
    markup_overlap: classification_loop.DynamicOverlap,
    check_overlap: classification_loop.Overlap,
    assignment_check_sample: Optional[evaluation.AssignmentCheckSample],
    display_formula: bool = False,
) -> str:
    formula = AnnotationPriceFormula(
        input_objects_count=input_objects_count,
        markup_config=markup_config,
        check_config=check_config,
        markup_overlap=markup_overlap,
        check_overlap=check_overlap,
        assignment_check_sample=assignment_check_sample,
    )
    if display_formula:
        from IPython.display import display, Math

        print('some edge cases may be not fully accounted')
        display(*(Math(f) for f in formula.clear_formula()))
    return formula.total_price_simple_raw_str


# current formula is not 100% precise:
# - it does not know about custom toloka commission
# - it rounds total price by cents, but is seems that Toloka using ceil() for it
# also, we have two variants of formulas
# - clear one, which does not account case than assignment price is such that commission is greater than default
#   because of min commission, and also incomplete last assignment
# - precise but complex one, which accounts min commission
@dataclass
class PoolPriceFormula:
    input_objects_count: int
    config: PoolPricingConfig
    overlap: classification_loop.Overlap

    def __post_init__(self):
        min_overlap = self.overlap.min_overlap
        self.has_price_range = isinstance(self.overlap, classification_loop.DynamicOverlap)
        max_overlap = self.overlap.max_overlap if self.has_price_range else min_overlap

        self.task_price = self.config.assignment_price / self.config.real_tasks_count

        base_price_clear = self.input_objects_count * self.task_price * (1 + toloka_commission)

        self.min_total_price_clear = base_price_clear * min_overlap
        self.max_total_price_clear = base_price_clear * max_overlap

        self.assignments_count = math.ceil(self.input_objects_count / self.config.real_tasks_count)
        self.assignments_price = self.config.assignment_price + max(
            self.config.assignment_price * toloka_commission, min_toloka_commission
        )

        base_price_precise = self.assignments_count * self.assignments_price
        self.min_total_price_precise = base_price_precise * min_overlap
        self.max_total_price_precise = base_price_precise * max_overlap

        self.overlap_str = f'{self.overlap.min_overlap}'
        self.overlap_explanation_str = 'Overlap'
        self.total_price_simple_str = fr'{self.min_total_price_clear:.2f}\$'
        self.total_price_precise_str = fr'{self.min_total_price_precise:.2f}\$'
        self.total_price_precise_raw_str = f'{self.min_total_price_precise:.2f}$'

        if self.has_price_range:
            self.overlap_str = fr'[{self.overlap_str} \dots {self.overlap.max_overlap}]'
            self.overlap_explanation_str = r'[MinOverlap \dots MaxOverlap]'
            self.total_price_simple_str = fr'{self.total_price_simple_str} \dots {self.max_total_price_clear:.2f}\$'
            self.total_price_precise_str = fr'{self.total_price_precise_str} \dots {self.max_total_price_precise:.2f}\$'
            self.total_price_precise_raw_str = (
                f'{self.total_price_precise_raw_str} ... {self.max_total_price_precise:.2f}$'
            )

    def clear_formula(self) -> str:
        return (
            r'TotalPrice_{clear} = TaskCount * PricePerTask_\$ * '
            fr'{self.overlap_explanation_str} * (1 + TolokaCommission) = '
            fr'{self.input_objects_count} * {self.task_price:.4f}\$ * {self.overlap_str} * '
            fr'{1 + toloka_commission} = {self.total_price_simple_str}.'
        )

    def precise_formula(self) -> str:
        return (
            r'TotalPrice_{precise} = AssignmentCount * PricePerAssignment_\$ * '
            fr'{self.overlap_explanation_str} = \left \lceil \frac {{TaskCount}} {{TasksOnAssignment}} \right \rceil * '
            r'(PricePerAssignment_\$ + max(PricePerAssignment_\$ * TolokaCommission, MinTolokaCommission_\$) '
            fr'* {self.overlap_explanation_str} = \lceil {self.input_objects_count} / '
            fr'{self.config.real_tasks_count} \rceil * '
            fr'({self.config.assignment_price}\$ + max({self.config.assignment_price}\$ * '
            fr'{toloka_commission}, {min_toloka_commission}\$) * {self.overlap_str} = '
            fr'{self.assignments_count} * {self.assignments_price:.3f} * {self.overlap_str} = '
            fr'{self.total_price_precise_str}.'
        )


@dataclass
class AnnotationPriceFormula:
    input_objects_count: int
    markup_config: PoolPricingConfig
    check_config: PoolPricingConfig

    markup_overlap: classification_loop.DynamicOverlap
    check_overlap: classification_loop.Overlap

    assignment_check_sample: Optional[evaluation.AssignmentCheckSample]

    def __post_init__(self):
        min_markup_overlap = self.markup_overlap.min_overlap
        max_markup_overlap = self.markup_overlap.max_overlap

        min_check_overlap = self.check_overlap.min_overlap
        self.has_check_price_range = isinstance(self.check_overlap, classification_loop.DynamicOverlap)
        max_check_overlap = self.check_overlap.max_overlap if self.has_check_price_range else min_check_overlap

        self.markup_task_price = self.markup_config.assignment_price / self.markup_config.real_tasks_count
        self.check_task_price = self.check_config.assignment_price / self.check_config.real_tasks_count

        self.check_overlap_str = f'{self.check_overlap.min_overlap}'
        self.markup_overlap_str = fr'[{self.markup_overlap.min_overlap} \dots {self.markup_overlap.max_overlap}]'
        self.markup_overlap_explanation_str = r'[MinMarkupOverlap \dots MaxMarkupOverlap]'
        self.check_overlap_explanation_str = 'CheckOverlap'

        self.check_tasks_count_str = (
            fr'[{self.input_objects_count * min_markup_overlap} \dots {self.input_objects_count * max_markup_overlap}]'
        )
        self.check_tasks_count_explanation_str = (
            f'MarkupTaskCount * {self.markup_overlap_explanation_str} = '
            f'{self.input_objects_count} * {self.markup_overlap_str}'
        )

        sample_frac = 1.0
        if self.assignment_check_sample is not None:
            assert self.assignment_check_sample.max_tasks_to_check is not None
            sample_frac = self.assignment_check_sample.max_tasks_to_check / self.markup_config.real_tasks_count

            self.check_tasks_count_str = (
                fr'[{math.ceil(self.input_objects_count * min_markup_overlap * sample_frac)} \dots '
                f'{math.ceil(self.input_objects_count * max_markup_overlap * sample_frac)}]'
            )
            self.check_tasks_count_explanation_str = (
                r'MarkupTaskCount * \frac {CheckSampleTaskCount} {MarkupTasksPerAssignment} * '
                fr'{self.markup_overlap_explanation_str} = {self.input_objects_count} * \frac '
                f'{{{self.assignment_check_sample.max_tasks_to_check}}} {{{self.markup_config.real_tasks_count}}} * '
                f'{self.markup_overlap_str}'
            )

        self.check_comission = toloka_commission

        if self.check_config.assignment_price * toloka_commission < min_toloka_commission:
            self.check_comission = min_toloka_commission / self.check_config.assignment_price

        self.markup_comission = toloka_commission

        if self.markup_config.assignment_price * toloka_commission < min_toloka_commission:
            self.markup_comission = min_toloka_commission / self.markup_config.assignment_price

        self.check_comission_explanation_str = (
            r'max \left( TolokaCommission, \frac {MinTolokaCommission} {PricePerCheckAssignment} \right) = '
            fr'max \left( {toloka_commission}, \frac {{{min_toloka_commission}}}'
            fr'{{{self.check_config.assignment_price}}} \right)'
        )
        self.markup_comission_explanation_str = (
            r'max \left( TolokaCommission, \frac {MinTolokaCommission} {PricePerMarkupAssignment} \right) = '
            fr'max \left( {toloka_commission}, \frac {{{min_toloka_commission}}}'
            fr'{{{self.markup_config.assignment_price}}} \right)'
        )

        base_markup_price_clear = self.input_objects_count * self.markup_task_price * (1 + self.markup_comission)
        # todo: sample is from whole assignment, there may be edge effects - not precise formula
        base_check_price_clear = (
            math.ceil(self.input_objects_count * sample_frac) * self.check_task_price * (1 + self.check_comission)
        )
        self.min_markup_price = base_markup_price_clear * min_markup_overlap
        self.max_markup_price = base_markup_price_clear * max_markup_overlap
        self.min_check_price = base_check_price_clear * min_check_overlap * min_markup_overlap
        self.max_check_price = base_check_price_clear * max_check_overlap * max_markup_overlap
        self.min_total_price_clear = self.min_markup_price + self.min_check_price
        self.max_total_price_clear = self.max_markup_price + self.max_check_price

        self.check_price_str = fr'{self.min_check_price:.2f}\$ \dots {self.max_check_price:.2f}\$'
        self.markup_price_str = fr'{self.min_markup_price:.2f}\$ \dots {self.max_markup_price:.2f}\$'
        self.total_price_simple_str = fr'{self.min_total_price_clear:.2f}\$ \dots {self.max_total_price_clear:.2f}\$'
        self.total_price_simple_raw_str = f'{self.min_total_price_clear:.2f}$ ... {self.max_total_price_clear:.2f}$'
        if self.has_check_price_range:
            self.check_overlap_str = fr'[{self.check_overlap_str} \dots {self.check_overlap.max_overlap}]'
            self.check_overlap_explanation_str = r'[MinCheckOverlap \dots MaxCheckOverlap]'

    def clear_formula(self) -> Tuple[str, ...]:
        return (
            f'MarkupTolokaComission = {self.markup_comission_explanation_str} = {self.markup_comission};',
            fr'MarkupPrice = MarkupTaskCount * PricePerMarkupTask_\$ * '
            fr'{self.markup_overlap_explanation_str} * (1 + MarkupTolokaComission) = '
            fr'{self.input_objects_count} * {self.markup_task_price:.4f}\$ * {self.markup_overlap_str} * '
            fr'{1 + self.markup_comission} = {self.markup_price_str};',
            f'CheckTaskCount = {self.check_tasks_count_explanation_str} = {self.check_tasks_count_str};',
            f'CheckTolokaComission = {self.check_comission_explanation_str} = {self.check_comission};',
            r'CheckPrice = CheckTaskCount * PricePerCheckTask_\$ * '
            fr'{self.check_overlap_explanation_str} * (1 + CheckTolokaComission) = '
            fr'{self.check_tasks_count_str} * {self.check_task_price:.4f}\$ * {self.check_overlap_str} * '
            fr'{1 + self.check_comission} = {self.check_price_str};',
            r'TotalPrice = MarkupPrice + CheckPrice = '
            fr'{self.markup_price_str} + {self.check_price_str} = {self.total_price_simple_str}.',
        )

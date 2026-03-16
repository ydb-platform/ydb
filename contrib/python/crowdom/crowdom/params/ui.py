from copy import copy
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Dict, Tuple, Callable

from IPython.display import display
import ipywidgets as widgets
import pandas as pd
import toloka.client as toloka

from .. import task_spec as spec
from . import params
from .base import ParamName, Update, UIEvent, SpeedControlChangeType, SpeedControlUpdate, SpeedControlEvent
from .client import Params, AnnotationParams
from .event_loop import EventLoop, get_loop, AnnotationEventLoop, get_loop_for_annotation
from .util import get_rec_label
from .characteristics import _get_cumulative_stats_and_characteristics


def get_inner_widget(param: params.Param) -> widgets.Widget:
    if isinstance(param, params.Overlap):
        return widgets.Combobox(
            value=param.value,
            placeholder='2-3',
            options=param.available_range,
            ensure_option=False,
            disabled=param.disabled,
            continuous_update=False,
        )

    obj = param.available_range[0]
    if isinstance(obj, str):
        widget = widgets.Dropdown(
            options=param.available_range,
            disabled=param.disabled,
            value=param.value,
        )
    elif isinstance(obj, bool):
        widget = widgets.Checkbox(
            disabled=param.disabled,
            value=param.value,
        )
    elif isinstance(obj, int):
        if isinstance(param.value, timedelta):
            value = int(param.value.total_seconds())
        else:
            value = param.value
        widget = widgets.BoundedIntText(
            min=param.available_range[0],
            max=param.available_range[-1],
            step=1,
            disabled=param.disabled,
            value=value,
        )
    elif isinstance(obj, float):
        widget = widgets.BoundedFloatText(
            min=param.available_range[0],
            max=param.available_range[-1],
            step=0.01,
            disabled=param.disabled,
            value=param.value,
            readout=True,
            readout_format='.2f',
        )
    else:
        raise NotImplementedError(f'{obj}')
    return widget


def get_speed_control_widget(
    param: params.SpeedControlList, loop: EventLoop, update_callback: Callable
) -> widgets.Widget:
    grid = widgets.GridspecLayout(param.list_size + 1, 1, align_items='center')

    fmt = widgets.HTML('<h5>Format: 1d 21h 35m 60s</h5>', layout={'width': '300px'})
    ratio = widgets.HTML('<h5>Ratio threshold</h5>', layout={'width': '294px'})
    reject = widgets.HTML('<h5>Reject?</h5>')
    help_button = widgets.Button(description='?', layout={'width': '30px'}, button_style='info')

    def on_button_clicked(button):
        loop.start(
            UIEvent(
                sender=ParamName.HELP.value,
                update={
                    ParamName.HELP: Update(f'<b>Help for {param.description}</b>: {param.help_msg}', button.disabled)
                },
            )
        )
        if loop.ui_initialized:
            update_callback()

    help_button.on_click(on_button_clicked)
    header = widgets.Box([fmt, ratio, reject, help_button])
    grid[0, :] = header

    def get_row(index: int, value: params.BlockTimePicker = copy(params.default_block)) -> widgets.Widget:
        time_widget = widgets.Text(
            value=value.time_value,
            continuous_update=False,
        )
        reject = widgets.Checkbox(
            value=value.reject,
            indent=False,
            layout=widgets.Layout(height='auto', width='40px', padding='0px 0px 0px 10px'),
        )
        ratio = widgets.BoundedFloatText(
            min=0.0,
            max=1.0,
            step=0.01,
            value=value.ratio,
        )

        remove_button = widgets.Button(
            icon='trash',
            button_style='danger',
            layout={'width': '30px'},
        )

        def on_time_change(change):
            s = change['new'].replace('y', 'Y')
            try:
                td = pd.Timedelta(s)
                assert param.min_value <= td.total_seconds() <= param.max_value
                loop.start(
                    params.SpeedControlEvent(
                        param.name.ui,
                        {
                            SpeedControlChangeType.TIME: SpeedControlUpdate(
                                change['new'], param.disabled, SpeedControlChangeType.TIME, index
                            )
                        },
                    )
                )
                if loop.ui_initialized:
                    update_callback()
            except:
                change['owner'].value = change['old']

        def on_ratio_change(change):
            loop.start(
                SpeedControlEvent(
                    param.name.ui,
                    {
                        SpeedControlChangeType.RATIO: SpeedControlUpdate(
                            change['new'], param.disabled, SpeedControlChangeType.RATIO, index
                        )
                    },
                )
            )
            if loop.ui_initialized:
                update_callback()

        def on_reject_change(change):
            loop.start(
                SpeedControlEvent(
                    param.name.ui,
                    {
                        SpeedControlChangeType.REJECT: SpeedControlUpdate(
                            change['new'], param.disabled, SpeedControlChangeType.REJECT, index
                        )
                    },
                )
            )
            if loop.ui_initialized:
                update_callback()

        def on_remove_click(button):
            loop.start(
                SpeedControlEvent(
                    param.name.ui,
                    {
                        SpeedControlChangeType.REMOVE: SpeedControlUpdate(
                            '', param.disabled, SpeedControlChangeType.REMOVE, index
                        )
                    },
                )
            )
            if loop.ui_initialized:
                update_callback()

        time_widget.observe(on_time_change, 'value')
        ratio.observe(on_ratio_change, 'value')
        reject.observe(on_reject_change, 'value')
        remove_button.on_click(on_remove_click)
        return widgets.Box([time_widget, ratio, reject, remove_button])

    grid.reserved_rows = [get_row(i) for i in range(param.list_size)]

    grid.add_button = widgets.Button(
        description='+',
        button_style='success',
        layout={'width': '686px'},
    )

    def on_add_click(button):
        loop.start(
            SpeedControlEvent(
                param.name.ui,
                {
                    SpeedControlChangeType.ADD: SpeedControlUpdate(
                        '',
                        param.disabled,
                        SpeedControlChangeType.ADD,
                    )
                },
            )
        )
        if loop.ui_initialized:
            update_callback()

    grid.add_button.on_click(on_add_click)
    return grid


def get_default_widget(param: params.Param, loop: EventLoop, update_callback: Callable) -> widgets.Widget:
    obj = param.available_range[0]
    widget = get_inner_widget(param)
    desc = widgets.HTML(value=f"<font color='black'>{param.description}</font>")
    help_button = widgets.Button(description='?', layout={'width': '30px'}, button_style='info')

    def on_button_clicked(button):
        loop.start(
            UIEvent(
                sender=ParamName.HELP.value,
                update={
                    ParamName.HELP: Update(f'<b>Help for {param.description}</b>: {param.help_msg}', button.disabled)
                },
            )
        )
        if loop.ui_initialized:
            update_callback()

    help_button.on_click(on_button_clicked)
    rec = widgets.HTML(value=f"<font color='black'>{get_rec_label(obj, param.recommended_range)}</font>")
    outer_widget = widgets.Box(
        [widgets.Box([desc, help_button], layout={'justify_content': 'space-between'}), widget, rec]
    )

    def on_change(change):
        loop.start(UIEvent(param.name.ui, {param.name: Update(change['new'], param.disabled)}))
        if loop.ui_initialized:
            update_callback()

    widget.observe(on_change, 'value')
    return outer_widget


def get_widget(param: params.Param, loop: EventLoop, update_callback: Callable) -> widgets.Widget:
    create_function = {
        params.Help: lambda *_: widgets.HTML('', layout={'height': '60px'}),
        params.Stat: lambda *_: widgets.HBox([widgets.Label(value=param.description), widgets.IntProgress()]),
        params.StatWithExtraText: lambda *_: widgets.HBox(
            [widgets.Label(value=param.description), widgets.IntProgress(), widgets.HTMLMath('')]
        ),
        params.Characteristics: lambda *_: widgets.HTMLMath(''),
        params.SpeedControlList: get_speed_control_widget,
    }.get(type(param), get_default_widget)
    w = create_function(param, loop, update_callback)
    dispatch(param, w)
    return w


def dispatch_default(param: params.Param, widget: widgets.Widget):
    w = widget.children[1]
    obj = param.available_range[0]
    color = 'black'
    if isinstance(obj, str) or isinstance(obj, bool):
        if param.value not in param.recommended_range:
            color = 'red'
    else:
        w.min = param.available_range[0]
        w.max = param.available_range[-1]

        if not (param.recommended_range[0] <= param.value <= param.recommended_range[-1]):
            color = 'red'

    widget.children[-1].value = f"<font color='{color}'>{get_rec_label(obj, param.recommended_range)}</font>"
    w.value, w.disabled = param.value, param.disabled


def dispatch_assignment_price(param: params.AssignmentPrice, widget: widgets.Widget):
    dispatch_default(param, widget)
    # if min bound for widget is 0.005, with step 0.01, values like 1.25 are "incorrect",
    # because they cannot be reached by ipywidgets logic, which contradicts toloka's logic with dynamic price step

    # however, there is still one edge effect - if param.value=1$, steps for widget increase and decrease
    # logically should be different. this cannot be done, as widget can have only one step value
    # so, current behavior is:
    # 0.9998 -> 0.9999 -> 1.0 -> [1.001 -> automatically updates in Param logic to] 1.01
    # and 1.01 -> 1.0 -> 0.999
    widget.children[1].step = params.get_price_step(param.value)


def dispatch_help(param: params.Help, widget: widgets.Widget):
    widget.value = param.value


def dispatch_stat(param: params.Stat, widget: widgets.Widget):
    v = param.value
    v = max(0.0, v)
    v = min(1.0, v)

    widget.children[1].value = v * 100

    b = 0
    if v < 0.5:
        r = 255
        g = int(2 * 255 * v)
    else:
        v = min(v, 1.0)
        g = 255
        r = int(255 * (1.0 - 2 * (v - 0.5)))

    if param.reverse:
        r, g = g, r

    widget.children[1].style.bar_color = '#%02x%02x%02x' % (r, g, b)


def dispatch_stat_with_extra_text(param: params.StatWithExtraText, widget: widgets.Widget):
    value, text = param.value
    stat = params.Stat(value=value, description=None, name=None)
    dispatch_stat(stat, widget)
    widget.children[-1].value = text


def dispatch_characteristics(param: params.Characteristics, widget: widgets.Widget):
    widget.value = '<br>'.join(param.value)


def dispatch_task_duration(param: params.TaskDuration, widget: widgets.Widget):
    widget.value = int(param.value.total_seconds())


def dispatch_speed_control(param: params.SpeedControlList, widget: widgets.Widget):
    widget.add_button.disabled = param.disabled
    for i, (row, item) in enumerate(zip(widget.reserved_rows, param.value)):
        row.children[0].value, row.children[0].disabled = item.time_value, param.disabled
        row.children[1].value, row.children[1].disabled = item.ratio, param.disabled
        row.children[2].value, row.children[2].disabled = item.reject, param.disabled
        row.children[3].disabled = param.disabled
        widget[i + 1, :] = row

    if len(param.value) < param.list_size:
        widget[len(param.value) + 1, :] = widget.add_button

    for i in range(len(param.value) + 2, param.list_size + 1):
        widget[i, :] = widgets.Label()


def dispatch(param: params.Param, widget: widgets.Widget):
    dispatch_function = {
        params.Help: dispatch_help,
        params.Stat: dispatch_stat,
        params.StatWithExtraText: dispatch_stat_with_extra_text,
        params.Characteristics: dispatch_characteristics,
        params.TaskDuration: dispatch_task_duration,
        params.SpeedControlList: dispatch_speed_control,
        params.AssignmentPrice: dispatch_assignment_price,
        params.AnnotationAssignmentPrice: dispatch_assignment_price,
    }.get(type(param), dispatch_default)
    dispatch_function(param, widget)


@dataclass
class UI:
    loop: EventLoop
    ui: widgets.Widget = field(init=False)
    param_to_widget: Dict[ParamName, widgets.Widget] = field(init=False)

    def __post_init__(self):
        self.param_to_widget = {
            p.name: get_widget(p, self.loop, self.update_widgets)
            for p in self.loop.params + self.loop.stats + [self.loop.characteristics]
            if isinstance(p, params.Param)
        }
        self.ui = create_ui(self.param_to_widget)
        self.loop.ui_initialized = True

    def update_widgets(self):
        for param in self.loop.params + self.loop.stats + [self.loop.characteristics]:
            if isinstance(param, params.Param):
                dispatch(param, self.param_to_widget[param.name])

    def get_params(self) -> Params:
        return self.loop.get_params()


@dataclass
class UIAnnotation:
    check_loop: EventLoop
    markup_loop: AnnotationEventLoop
    ui: widgets.Widget = field(init=False)
    check_param_to_widget: Dict[ParamName, widgets.Widget] = field(init=False)
    markup_param_to_widget: Dict[ParamName, widgets.Widget] = field(init=False)
    cumulative_param_to_widget: Dict[ParamName, widgets.Widget] = field(init=False)
    cumulative_stats: List[params.Stat] = field(init=False)
    cumulative_characteristics: params.Characteristics = field(init=False)

    def __post_init__(self):

        self.check_param_to_widget = {
            p.name: get_widget(p, self.check_loop, self.update_widgets)
            for p in self.check_loop.params + self.check_loop.stats + [self.check_loop.characteristics]
            if isinstance(p, params.Param)
        }
        self.markup_param_to_widget = {
            p.name: get_widget(p, self.markup_loop, self.update_widgets)
            for p in self.markup_loop.params + self.markup_loop.stats + [self.markup_loop.characteristics]
            if isinstance(p, params.Param)
        }
        check_ui = create_ui(self.check_param_to_widget)
        markup_ui = create_markup_ui(self.markup_param_to_widget)
        ui = widgets.Accordion(children=[markup_ui, check_ui])
        ui.set_title(0, 'Annotation')
        ui.set_title(1, 'Check')

        ui.selected_index = 0
        cumulative_quality = params.StatWithExtraText(ParamName.QUALITY, 'Quality')
        cumulative_time = params.Stat(ParamName.TIME, 'Time', reverse=True)
        cumulative_cost = params.Stat(ParamName.COST, 'Cost', reverse=True)
        self.cumulative_stats = [cumulative_quality, cumulative_time, cumulative_cost]
        self.cumulative_characteristics = params.Characteristics()
        self.cumulative_param_to_widget = {
            p.name: get_widget(p, None, lambda: None) for p in self.cumulative_stats + [self.cumulative_characteristics]
        }
        cumulative_stats_w = get_grid([self.cumulative_param_to_widget[p.name] for p in self.cumulative_stats])

        cumulative_characteristics_w = self.cumulative_param_to_widget[self.cumulative_characteristics.name]

        self.ui = widgets.VBox([ui, cumulative_stats_w, cumulative_characteristics_w])
        self.update_widgets()
        self.check_loop.ui_initialized = True
        self.markup_loop.ui_initialized = True

    def update_widgets(self):
        for loop, param_to_widget in [
            (self.check_loop, self.check_param_to_widget),
            (self.markup_loop, self.markup_param_to_widget),
        ]:
            for param in loop.params + loop.stats + [loop.characteristics]:
                if isinstance(param, params.Param):
                    dispatch(param, param_to_widget[param.name])

        check_cumulative_params = {
            **{
                'check_' + name.value: self.check_loop.state[name].value
                for name in [
                    ParamName.TASK_COUNT,
                    ParamName.CONTROL_TASK_COUNT,
                    ParamName.ASSIGNMENT_PRICE,
                    ParamName.OVERLAP,
                    ParamName.OVERLAP_CONFIDENCE,
                ]
            },
            **{'check_' + p.name.value: p.value for p in self.check_loop.stats},
        }
        markup_cumulative_params = {
            **{
                'markup_' + name.value: self.markup_loop.state[name].value
                for name in [
                    ParamName.TASK_COUNT,
                    ParamName.ASSIGNMENT_PRICE,
                    ParamName.OVERLAP,
                    ParamName.OVERLAP_CONFIDENCE,
                    ParamName.CHECK_SAMPLE,
                    ParamName.MAX_TASKS_TO_CHECK,
                    ParamName.ACCURACY_THRESHOLD,
                ]
            },
            **{'markup_' + p.name.value: p.value for p in self.markup_loop.stats},
        }
        stats, characteristics = _get_cumulative_stats_and_characteristics(
            **check_cumulative_params, **markup_cumulative_params
        )
        for p, v in zip(self.cumulative_stats, stats):
            p.value = v
            dispatch(p, self.cumulative_param_to_widget[p.name])

        self.cumulative_characteristics.value = characteristics
        dispatch(self.cumulative_characteristics, self.cumulative_param_to_widget[self.cumulative_characteristics.name])

    def get_params(self) -> Tuple[Params, AnnotationParams]:
        return self.check_loop.get_params(), self.markup_loop.get_params()


def get_grid(ws: List[widgets.Widget]) -> widgets.Widget:
    grid = widgets.GridspecLayout(len(ws), 3)
    for i, w in enumerate(ws):
        try:
            for j, c in enumerate(w.children):
                grid[i, j] = c
        except:
            grid[i, :] = w
    return grid


def create_ui(ws: Dict[ParamName, widgets.Widget]) -> widgets.Widget:
    general_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.TASK_DURATION_HINT,
                ParamName.LANGUAGE,
                ParamName.TASK_COUNT,
                ParamName.ASSIGNMENT_PRICE,
            ]
        ]
    )

    control_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.QUALITY_PRESET,
                ParamName.CONTROL_TASK_COUNT,
                ParamName.CONTROL_TASKS_FOR_ACCEPT,
                ParamName.CONTROL_TASKS_FOR_BLOCK,
                ParamName.OVERLAP,
                ParamName.OVERLAP_CONFIDENCE,
                ParamName.AGGREGATION_ALGORITHM,
            ]
        ]
    )
    qual_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.VERIFIED_LANG,
                ParamName.AGE_RESTRICTION,
                ParamName.TRAINING,
                ParamName.TRAINING_SCORE,
            ]
        ]
    )
    speed_control = ws[ParamName.SPEED_CONTROL]

    tab = widgets.Tab()
    tab.children = [general_grid, qual_grid, control_grid, speed_control]
    titles = ('General', 'Qualifications', 'Control', 'Speed control')

    for i, title in enumerate(titles):
        tab.set_title(i, title)

    tab.layout.height = '348px'

    stats = get_grid([ws[name] for name in [ParamName.QUALITY, ParamName.TIME, ParamName.COST]])
    chars = ws[ParamName.CHARACTERISTICS]
    ui = widgets.VBox([ws[ParamName.HELP], tab, stats, chars])
    return ui


def create_markup_ui(ws: Dict[ParamName, widgets.Widget]) -> widgets.Widget:
    general_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.TASK_DURATION_HINT,
                ParamName.LANGUAGE,
                ParamName.TASK_COUNT,
                ParamName.ASSIGNMENT_PRICE,
            ]
        ]
    )

    control_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.QUALITY_PRESET,
                ParamName.CONTROL_TASKS_FOR_ACCEPT,
                ParamName.CONTROL_TASKS_FOR_BLOCK,
                ParamName.OVERLAP,
                ParamName.OVERLAP_CONFIDENCE,
            ]
        ]
    )
    check_sample_grid = get_grid(
        [
            ws[name]
            for name in [
                ParamName.CHECK_SAMPLE,
                ParamName.MAX_TASKS_TO_CHECK,
                ParamName.ACCURACY_THRESHOLD,
            ]
        ]
    )
    qual_grid = get_grid([ws[name] for name in [ParamName.VERIFIED_LANG, ParamName.AGE_RESTRICTION]])
    speed_control = ws[ParamName.SPEED_CONTROL]

    tab = widgets.Tab()
    tab.children = [general_grid, qual_grid, control_grid, check_sample_grid, speed_control]
    titles = ('General', 'Qualifications', 'Control', 'Check sample', 'Speed control')

    for i, title in enumerate(titles):
        tab.set_title(i, title)

    tab.layout.height = '275px'

    stats = get_grid([ws[name] for name in [ParamName.QUALITY, ParamName.TIME, ParamName.COST]])
    chars = ws[ParamName.CHARACTERISTICS]
    ui = widgets.VBox([ws[ParamName.HELP], tab, stats, chars])
    return ui


def get_interface(
    task_spec: spec.PreparedTaskSpec,
    task_duration_hint: timedelta,
    toloka_client: toloka.TolokaClient,
) -> UI:
    loop = get_loop(task_spec, task_duration_hint, toloka_client)
    ui = UI(loop)
    display(ui.ui)
    return ui


def get_annotation_interface(
    task_spec: spec.AnnotationTaskSpec,
    check_task_duration_hint: timedelta,
    annotation_task_duration_hint: timedelta,
    toloka_client: toloka.TolokaClient,
) -> UIAnnotation:
    check_loop = get_loop(task_spec.check, check_task_duration_hint, toloka_client)
    markup_loop = get_loop_for_annotation(task_spec, annotation_task_duration_hint, toloka_client)
    ui = UIAnnotation(check_loop, markup_loop)
    display(ui.ui)
    return ui

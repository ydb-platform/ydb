import asyncio
import base64
import inspect
import time
from collections import defaultdict
from contextlib import suppress
from functools import partial

import numpy as np
from bokeh.models import (
    BoxEditTool,
    Button,
    CustomJS,
    DataRange1d,
    DatetimeAxis,
    FactorRange,
    FreehandDrawTool,
    PointDrawTool,
    PolyDrawTool,
    PolyEditTool,
    Range1d,
)
from panel.io.notebook import push_on_root
from panel.io.state import set_curdoc, state
from panel.pane import panel

from ...core.data import Dataset
from ...core.options import CallbackError
from ...core.util import (
    VersionError,
    datetime_types,
    dimension_sanitizer,
    dt64_to_dt,
    isequal,
)
from ...element import Table
from ...streams import (
    BoundsX,
    BoundsXY,
    BoundsY,
    BoxEdit,
    CDSStream,
    CurveEdit,
    DoubleTap,
    Draw,
    FreehandDraw,
    Lasso,
    MouseEnter,
    MouseLeave,
    MultiAxisTap,
    PanEnd,
    PlotReset,
    PlotSize,
    PointDraw,
    PointerX,
    PointerXY,
    PointerY,
    PolyDraw,
    PolyEdit,
    PressUp,
    RangeX,
    RangeXY,
    RangeY,
    Selection1D,
    SelectionXY,
    SelectMode,
    SingleTap,
    Stream,
    Tap,
)
from ...util.warnings import warn
from .util import BOKEH_GE_3_3_0, BOKEH_GE_3_4_0, convert_timestamp

if BOKEH_GE_3_4_0:
    from bokeh.models import XY, Panel as BokehPanel

POPUP_POSITION_ANCHOR = {
    "top_right": "bottom_left",
    "top_left": "bottom_right",
    "bottom_left": "top_right",
    "bottom_right": "top_left",
    "right": "top_left",
    "left": "top_right",
    "top": "bottom",
    "bottom": "top",
}


class Callback:
    """Provides a baseclass to define callbacks, which return data from
    bokeh model callbacks, events and attribute changes. The callback
    then makes this data available to any streams attached to it.

    The definition of a callback consists of a number of components:

    * models      : Defines which bokeh models the callback will be
                    attached on referencing the model by its key in
                    the plots handles, e.g. this could be the x_range,
                    y_range, plot, a plotting tool or any other
                    bokeh mode.

    * attributes  : The attributes define which attributes to send
                    back to Python. They are defined as a dictionary
                    mapping between the name under which the variable
                    is made available to Python and the specification
                    of the attribute. The specification should start
                    with the variable name that is to be accessed and
                    the location of the attribute separated by
                    periods.  All models defined by the models and can
                    be addressed in this way, e.g. to get the start of
                    the x_range as 'x' you can supply {'x':
                    'x_range.attributes.start'}. Additionally certain
                    handles additionally make the cb_obj variables
                    available containing additional information about
                    the event.

    * on_events   : If the Callback should listen to bokeh events this
                    should declare the types of event as a list (optional)

    * on_changes  : If the Callback should listen to model attribute
                    changes on the defined ``models`` (optional)

    If either on_events or on_changes are declared the Callback will
    be registered using the on_event or on_change machinery, otherwise
    it will be treated as a regular callback on the model.  The
    callback can also define a _process_msg method, which can modify
    the data sent by the callback before it is passed to the streams.
    """

    # Attributes to sync
    attributes = {}

    # Debounce in milliseconds
    debounce = None

    # The plotting handle(s) to attach the JS callback on
    models = []

    # Additional handles to hash on for uniqueness
    extra_handles = []

    # Conditions when callback should be skipped
    skip_events  = []
    skip_changes = []

    # Callback will listen to events of the supplied type on the models
    on_events = []

    # List of change events on the models to listen to
    on_changes = []

    # Internal state
    _callbacks = {}
    _transforms = []

    # Asyncio background task
    _background_task = set()

    def __init__(self, plot, streams, source, **params):
        self.plot = plot
        self.streams = streams
        self.source = source
        self.handle_ids = defaultdict(dict)
        self.reset()
        self._active = False
        self._prev_msg = None

    def _transform(self, msg):
        for transform in self._transforms:
            msg = transform(msg, self)
        return msg

    def _process_msg(self, msg):
        """Subclassable method to preprocess JSON message in callback
        before passing to stream.

        """
        return self._transform(msg)

    def cleanup(self):
        self.reset()
        self.handle_ids = None
        self.plot = None
        self.source = None
        self.streams = []
        Callback._callbacks = {k: cb for k, cb in Callback._callbacks.items()
                               if cb is not self}

    def reset(self):
        if self.handle_ids:
            handles = self._init_plot_handles()
            for handle_name in self.models:
                if handle_name not in handles:
                    continue
                handle = handles[handle_name]
                cb_hash = (id(handle), id(type(self)))
                self._callbacks.pop(cb_hash, None)
        self.plot_handles = {}
        self._queue = []
        self._event_queue = []

    def _filter_msg(self, msg, ids):
        """Filter event values that do not originate from the plotting
        handles associated with a particular stream using their
        ids to match them.

        """
        filtered_msg = {}
        for k, v in msg.items():
            if isinstance(v, dict) and 'id' in v:
                if v['id'] in ids:
                    filtered_msg[k] = v['value']
            else:
                filtered_msg[k] = v
        return filtered_msg

    async def on_msg(self, msg):
        streams = []
        for stream in self.streams:
            handle_ids = self.handle_ids[stream]
            ids = list(handle_ids.values())
            filtered_msg = self._filter_msg(msg, ids)
            processed_msg = self._process_msg(filtered_msg)
            if not processed_msg:
                continue
            stream.update(**processed_msg)
            stream._metadata = {h: {'id': hid, 'events': self.on_events}
                                for h, hid in handle_ids.items()}
            streams.append(stream)

        try:
            with set_curdoc(self.plot.document):
                Stream.trigger(streams)
        except CallbackError as e:
            if self.plot.root and self.plot.root.ref['id'] in state._handles:
                handle, _ = state._handles[self.plot.root.ref['id']]
                handle.update({'text/html': str(e)}, raw=True)
            else:
                raise e
        except Exception as e:
            raise e
        finally:
            for stream in streams:
                stream._metadata = {}

    def _init_plot_handles(self):
        """Find all requested plotting handles and cache them along
        with the IDs of the models the callbacks will be attached to.

        """
        plots = [self.plot]
        if self.plot.subplots:
            plots += list(self.plot.subplots.values())

        handles = {}
        for plot in plots:
            for k, v in plot.handles.items():
                handles[k] = v
        self.plot_handles = handles

        requested = {}
        for h in self.models+self.extra_handles:
            if h in self.plot_handles:
                requested[h] = handles[h]
        self.handle_ids.update(self._get_stream_handle_ids(requested))
        return requested

    def _get_stream_handle_ids(self, handles):
        """Gather the ids of the plotting handles attached to this callback
        This allows checking that a stream is not given the state
        of a plotting handle it wasn't attached to

        """
        stream_handle_ids = defaultdict(dict)
        for stream in self.streams:
            for h in self.models+self.extra_handles:
                if h in handles:
                    handle_id = handles[h].ref['id']
                    stream_handle_ids[stream][h] = handle_id
        return stream_handle_ids

    @classmethod
    def resolve_attr_spec(cls, spec, cb_obj, model=None):
        """Resolves a Callback attribute specification looking the
        corresponding attribute up on the cb_obj, which should be a
        bokeh model. If not model is supplied cb_obj is assumed to
        be the same as the model.

        """
        if not cb_obj:
            raise AttributeError(f'Bokeh plot attribute {spec} could not be found')
        if model is None:
            model = cb_obj
        spec = spec.split('.')
        resolved = cb_obj
        for p in spec[1:]:
            if p == 'attributes':
                continue
            if isinstance(resolved, dict):
                resolved = resolved.get(p)
            else:
                resolved = getattr(resolved, p, None)
        return {'id': model.ref['id'], 'value': resolved}

    def skip_event(self, event):
        return any(skip(event) for skip in self.skip_events)

    def skip_change(self, msg):
        return any(skip(msg) for skip in self.skip_changes)

    def _set_busy(self, busy):
        """Sets panel.state to busy if available.

        """
        if 'busy' not in state.param:
            return # Check if busy state is supported

        from panel.util import edit_readonly
        with edit_readonly(state):
            state.busy = busy

    async def on_change(self, attr, old, new):
        """Process change events adding timeout to process multiple concerted
        value change at once rather than firing off multiple plot updates.

        """
        self._queue.append((attr, old, new, time.perf_counter()))
        if not self._active and self.plot.document:
            self._active = True
            self._set_busy(True)
            with set_curdoc(self.plot.document):
                state.execute(self.process_on_change)

    async def on_event(self, event):
        """Process bokeh UIEvents adding timeout to process multiple concerted
        value change at once rather than firing off multiple plot updates.

        """
        self._event_queue.append((event, time.perf_counter()))
        if not self._active and self.plot.document:
            self._active = True
            self._set_busy(True)
            with set_curdoc(self.plot.document):
                state.execute(self.process_on_event)

    async def process_on_event(self, timeout=None):
        """Trigger callback change event and triggering corresponding streams.

        """
        if self.debounce is not None:
            current = time.perf_counter()
            while any((current-e[-1]) < (self.debounce/1000) for e in self._queue+self._event_queue):
                await asyncio.sleep(0.05)
                current = time.perf_counter()
        else:
            await asyncio.sleep(0.01)

        if not self._event_queue:
            self._active = False
            self._set_busy(False)
            return

        # Get unique event types in the queue
        events = list(dict([(event.event_name, event)
                            for event, dt in self._event_queue]).values())
        self._event_queue = []

        # Process event types
        for event in events:
            if self.skip_event(event):
                continue
            msg = {}
            for attr, path in self.attributes.items():
                model_obj = self.plot_handles.get(self.models[0])
                msg[attr] = self.resolve_attr_spec(path, event, model_obj)
            await self.on_msg(msg)
        state.execute(self.process_on_event)

    async def process_on_change(self):
        # Give on_change time to process new events
        if self.debounce is not None:
            current = time.perf_counter()
            while any((current-e[-1]) < (self.debounce/1000) for e in self._queue+self._event_queue):
                await asyncio.sleep(0.05)
                current = time.perf_counter()
        else:
            await asyncio.sleep(0.01)

        if not self._queue:
            self._active = False
            self._set_busy(False)
            return
        self._queue = []

        msg = {}
        for attr, path in self.attributes.items():
            attr_path = path.split('.')
            if attr_path[0] == 'cb_obj':
                obj_handle = self.models[0]
                path = '.'.join(self.models[:1]+attr_path[1:])
            else:
                obj_handle = attr_path[0]
            cb_obj = self.plot_handles.get(obj_handle)
            try:
                msg[attr] = self.resolve_attr_spec(path, cb_obj)
            except Exception:
                # To give BokehJS a chance to update the model
                # https://github.com/holoviz/holoviews/issues/5746
                await asyncio.sleep(0.05)
                msg[attr] = self.resolve_attr_spec(path, cb_obj)

        if self.skip_change(msg):
            equal = True
        else:
            equal = isequal(msg, self._prev_msg)

        if not equal or any(s.transient for s in self.streams):
            await self.on_msg(msg)
            self._prev_msg = msg
        state.execute(self.process_on_change)

    def _schedule_event(self, event):
        if self.plot.comm or not self.plot.document.session_context or state._is_pyodide:
            task = asyncio.create_task(self.on_event(event))
            self._background_task.add(task)
            task.add_done_callback(self._background_task.discard)
        else:
            self.plot.document.add_next_tick_callback(partial(self.on_event, event))

    def _schedule_change(self, attr, old, new):
        if not self.plot.document:
            return
        if self.plot.comm or not self.plot.document.session_context or state._is_pyodide:
            task = asyncio.create_task(self.on_change(attr, old, new))
            self._background_task.add(task)
            task.add_done_callback(self._background_task.discard)
        else:
            self.plot.document.add_next_tick_callback(partial(self.on_change, attr, old, new))

    def set_callback(self, handle):
        """Set up on_change events for bokeh server interactions.

        """
        if self.on_events:
            for event in self.on_events:
                handle.on_event(event, self._schedule_event)
        if self.on_changes:
            for change in self.on_changes:
                if change in ['patching', 'streaming']:
                    # Patch and stream events do not need handling on server
                    continue
                handle.on_change(change, self._schedule_change)

    def initialize(self, plot_id=None):
        handles = self._init_plot_handles()
        hash_handles, cb_handles = [], []
        for handle_name in self.models+self.extra_handles:
            if handle_name not in handles:
                warn_args = (handle_name, type(self.plot).__name__,
                             type(self).__name__)
                print('{} handle not found on {}, cannot '
                      'attach {} callback'.format(*warn_args))
                continue
            handle = handles[handle_name]
            if handle_name not in self.extra_handles:
                cb_handles.append(handle)
            hash_handles.append(handle)

        # Hash the plot handle with Callback type allowing multiple
        # callbacks on one handle to be merged
        cb_hash = (*map(id, hash_handles), id(type(self)))
        if cb_hash in self._callbacks:
            # Merge callbacks if another callback has already been attached
            cb = self._callbacks[cb_hash]
            cb.streams = list(set(cb.streams+self.streams))
            for k, v in self.handle_ids.items():
                cb.handle_ids[k].update(v)
            self.cleanup()
            return

        for handle in cb_handles:
            self.set_callback(handle)
        self._callbacks[cb_hash] = self


class PointerXYCallback(Callback):
    """Returns the mouse x/y-position on mousemove event.

    """

    attributes = {'x': 'cb_obj.x', 'y': 'cb_obj.y'}
    models = ['plot']
    on_events = ['mousemove']

    def _process_out_of_bounds(self, value, start, end):
        """Clips out of bounds values

        """
        if isinstance(value, np.datetime64):
            v = dt64_to_dt(value)
            if isinstance(start, (int, float)):
                start = convert_timestamp(start)
            if isinstance(end, (int, float)):
                end = convert_timestamp(end)
            s, e = start, end
            if isinstance(s, np.datetime64):
                s = dt64_to_dt(s)
            if isinstance(e, np.datetime64):
                e = dt64_to_dt(e)
        else:
            v, s, e = value, start, end

        if v < s:
            value = start
        elif v > e:
            value = end

        return value

    def _process_msg(self, msg):
        x_range = self.plot.handles.get('x_range')
        y_range = self.plot.handles.get('y_range')
        xaxis = self.plot.handles.get('xaxis')
        yaxis = self.plot.handles.get('yaxis')

        if 'x' in msg and isinstance(xaxis, DatetimeAxis):
            msg['x'] = convert_timestamp(msg['x'])
        if 'y' in msg and isinstance(yaxis, DatetimeAxis):
            msg['y'] = convert_timestamp(msg['y'])

        if isinstance(x_range, FactorRange) and isinstance(msg.get('x'), (int, float)):
            with suppress(IndexError): # See: https://github.com/holoviz/holoviews/pull/6438
                msg['x'] = x_range.factors[int(msg['x'])]
        elif 'x' in msg and isinstance(x_range, (Range1d, DataRange1d)):
            xstart, xend = x_range.start, x_range.end
            if xstart > xend:
                xstart, xend = xend, xstart
            x = self._process_out_of_bounds(msg['x'], xstart, xend)
            if x is None:
                msg = {}
            else:
                msg['x'] = x

        if isinstance(y_range, FactorRange) and isinstance(msg.get('y'), (int, float)):
            with suppress(IndexError):
                msg['y'] = y_range.factors[int(msg['y'])]
        elif 'y' in msg and isinstance(y_range, (Range1d, DataRange1d)):
            ystart, yend = y_range.start, y_range.end
            if ystart > yend:
                ystart, yend = yend, ystart
            y = self._process_out_of_bounds(msg['y'], ystart, yend)
            if y is None:
                msg = {}
            else:
                msg['y'] = y

        return self._transform(msg)


class PointerXCallback(PointerXYCallback):
    """Returns the mouse x-position on mousemove event.

    """

    attributes = {'x': 'cb_obj.x'}


class PointerYCallback(PointerXYCallback):
    """Returns the mouse x/y-position on mousemove event.

    """

    attributes = {'y': 'cb_obj.y'}


class DrawCallback(PointerXYCallback):
    on_events = ['pan', 'panstart', 'panend']
    models = ['plot']
    attributes = {'x': 'cb_obj.x', 'y': 'cb_obj.y', 'event': 'cb_obj.event_name'}

    def __init__(self, *args, **kwargs):
        self.stroke_count = 0
        super().__init__(*args, **kwargs)

    def _process_msg(self, msg):
        event = msg.pop('event')
        if event == 'panend':
            self.stroke_count += 1
        return self._transform(dict(msg, stroke_count=self.stroke_count))


class PopupMixin:

    geom_type = 'any'

    def initialize(self, plot_id=None):
        super().initialize(plot_id=plot_id)
        if not self.streams:
            return

        self._selection_event = None
        self._processed_event = True
        self._skipped_partial_event = False
        self._existing_popup = None
        stream = self.streams[0]
        if not getattr(stream, 'popup', None):
            return
        elif not BOKEH_GE_3_4_0:
            raise VersionError("Popup requires Bokeh >= 3.4")

        close_button = Button(label="", stylesheets=[r"""
        :host(.bk-Button) {
            width: 100%;
            height: 100%;
            top: -1em;
        }
        .bk-btn, .bk-btn:hover, .bk-btn:active, .bk-btn:focus {
            background: none;
            border: none;
            color: inherit;
            cursor: pointer;
            padding: 0.5em;
            margin: -0.5em;
            outline: none;
            box-shadow: none;
            position: absolute;
            top: 0;
            right: 0;
        }
        .bk-btn::after {
            content: '\2715';
        }
        """],
        css_classes=["popup-close-btn"])
        self._popup_position = stream.popup_position
        self._panel = BokehPanel(
            position=XY(x=np.nan, y=np.nan),
            anchor=stream.popup_anchor or POPUP_POSITION_ANCHOR[self._popup_position],
            elements=[close_button],
            visible=False,
            styles={"zIndex": "1000"},
        )
        close_button.js_on_click(CustomJS(args=dict(panel=self._panel), code="panel.visible = false"))

        self.plot.state.elements.append(self._panel)
        self._watch_position()

    def _watch_position(self):
        geom_type = self.geom_type
        self.plot.state.on_event('selectiongeometry', self._update_selection_event)
        self.plot.state.js_on_event('selectiongeometry', CustomJS(
            args=dict(panel=self._panel, popup_position=self._popup_position),
            code=f"""
            export default ({{panel, popup_position}}, cb_obj, _) => {{
                const el = panel.elements[1];
                if ((el && !el.visible) || !cb_obj.final || ({geom_type!r} !== 'any' && cb_obj.geometry.type !== {geom_type!r})) {{
                    return;
                }}

                let pos;
                if (cb_obj.geometry.type === 'point') {{
                    pos = {{x: cb_obj.geometry.x, y: cb_obj.geometry.y}};
                }} else if (cb_obj.geometry.type === 'rect') {{
                    let x, y;
                    if (popup_position.includes('left')) {{
                        x = cb_obj.geometry.x0;
                    }} else if (popup_position.includes('right')) {{
                        x = cb_obj.geometry.x1;
                    }} else {{
                        x = (cb_obj.geometry.x0 + cb_obj.geometry.x1) / 2;
                    }}
                    if (popup_position.includes('top')) {{
                        y = cb_obj.geometry.y1;
                    }} else if (popup_position.includes('bottom')) {{
                        y = cb_obj.geometry.y0;
                    }} else {{
                        y = (cb_obj.geometry.y0 + cb_obj.geometry.y1) / 2;
                    }}
                    pos = {{x: x, y: y}};
                }} else if (cb_obj.geometry.type === 'poly') {{
                    let x, y;
                    if (popup_position.includes('left')) {{
                        x = Math.min(...cb_obj.geometry.x);
                    }} else if (popup_position.includes('right')) {{
                        x = Math.max(...cb_obj.geometry.x);
                    }} else {{
                        x = (Math.min(...cb_obj.geometry.x) + Math.max(...cb_obj.geometry.x)) / 2;
                    }}
                    if (popup_position.includes('top')) {{
                        y = Math.max(...cb_obj.geometry.y);
                    }} else if (popup_position.includes('bottom')) {{
                        y = Math.min(...cb_obj.geometry.y);
                    }} else {{
                        y = (Math.min(...cb_obj.geometry.y) + Math.max(...cb_obj.geometry.y)) / 2;
                    }}
                    pos = {{x: x, y: y}};
                }}

                if (pos) {{
                    panel.position.setv(pos);
                }}
            }}""",
        ))

    def _get_position(self, event):
        if self.geom_type not in ('any', event.geometry['type']):
            return
        elif event.geometry['type'] == 'point':
            return dict(x=event.geometry['x'], y=event.geometry['y'])
        elif event.geometry['type'] == 'rect':
            return dict(x=event.geometry['x1'], y=event.geometry['y1'])
        elif event.geometry['type'] == 'poly':
            return dict(x=np.max(event.geometry['x']), y=np.max(event.geometry['y']))

    def _update_selection_event(self, event):
        if (((prev:= self._selection_event) and prev.final and not self._processed_event) or
            self.geom_type not in (event.geometry["type"], "any")):
            return
        self._selection_event = event
        self._processed_event = not event.final
        if event.final and self._skipped_partial_event:
            if self.plot.document.session_context and self.plot.document.session_context.server_context:
                self.plot.document.add_next_tick_callback(self._process_selection_partial_event)
            else:
                state.execute(self._process_selection_partial_event)

    async def on_msg(self, msg):
        await super().on_msg(msg)
        if hasattr(self, '_panel'):
            await self._process_selection_event()

    async def _process_selection_event(self):
        event = self._selection_event
        if event is not None:
            if self.geom_type not in (event.geometry["type"], "any"):
                return
            elif not event.final:
                self._skipped_partial_event = True
                return

        if event:
            self._processed_event = True
        for stream in self.streams:
            popup = stream.popup
            if popup is not None:
                break

        popup_is_callable = callable(popup)
        if popup_is_callable:
            with set_curdoc(self.plot.document):
                if inspect.iscoroutinefunction(popup):
                    popup = await popup(**stream.contents)
                else:
                    popup = popup(**stream.contents)

        # If no popup is defined, hide the bokeh panel wrapper
        if popup is None:
            if self._panel.visible:
                self._panel.visible = False
            return

        popup_pane = panel(popup)
        # offer the user ability to control when the popup bk panel shows up
        # however, if the popup is not callable (singleton), we cannot do this
        # check--else, it'll never re-appear if they set `visible=False` once
        if popup_is_callable and not popup_pane.visible:
            return

        state.execute(partial(self._update_popup, event, popup_pane), schedule=True)

    def _update_popup(self, event, popup_pane):
        if not popup_pane.stylesheets:
            self._panel.stylesheets = [
                """
                :host {
                    padding: 1em;
                    border-radius: 0.5em;
                    border: 1px solid lightgrey;
                }
                """,
            ]
        else:
            self._panel.stylesheets = []

        self._panel.visible = True
        # for existing popup, important to check if they're visible
        # otherwise, UnknownReferenceError: can't resolve reference 'p...'
        # meaning the popup has already been removed; we need to regenerate
        if self._existing_popup and not self._existing_popup.visible:
            self._existing_popup.visible = True
            position = self._get_position(event) if event else None
            if position:
                self._panel.position = XY(**position)
                if self.plot.comm:  # update Jupyter Notebooks
                    push_on_root(self.plot.root.ref['id'])
            return

        model = popup_pane.get_root(self.plot.document, self.plot.comm)
        model.js_on_change('visible', CustomJS(
            args=dict(panel=self._panel),
            code="""
            export default ({panel}, event, _) => {
              if (!event.visible) {
                panel.visible = false;
              }
            }""",
        ))
        # the first element is the close button
        self._panel.elements = [self._panel.elements[0], model]
        if self.plot.comm:  # update Jupyter Notebook
            push_on_root(self.plot.root.ref['id'])
        self._existing_popup = popup_pane

    async def _process_selection_partial_event(self):
        await self._process_selection_event()
        self._skipped_partial_event = False


class TapCallback(PopupMixin, PointerXYCallback):
    """Returns the mouse x/y-position on tap event.

    """

    geom_type = 'point'

    on_events = ['tap', 'doubletap']

    def _process_out_of_bounds(self, value, start, end):
        """Sets out of bounds values to None

        """
        if isinstance(value, np.datetime64):
            v = dt64_to_dt(value)
            if isinstance(start, (int, float)):
                start = convert_timestamp(start)
            if isinstance(end, (int, float)):
                end = convert_timestamp(end)
            s, e = start, end
            if isinstance(s, np.datetime64):
                s = dt64_to_dt(s)
            if isinstance(e, np.datetime64):
                e = dt64_to_dt(e)
        else:
            v, s, e = value, start, end

        if v < s or v > e:
            value = None
        return value


class MultiAxisTapCallback(TapCallback):
    """Returns the mouse x/y-positions on tap event.

    """

    attributes = {'x': 'cb_obj.x', 'y': 'cb_obj.y'}

    def _process_msg(self, msg):
        x_range = self.plot.handles.get('x_range')
        y_range = self.plot.handles.get('y_range')
        extra_x = list(self.plot.handles.get('extra_x_ranges', {}).values())
        extra_y = list(self.plot.handles.get('extra_y_ranges', {}).values())
        xaxis = self.plot.handles.get('xaxis')
        yaxis = self.plot.handles.get('yaxis')

        # Compute x/y position relative to first axis
        x, y = msg['x'], msg['y']
        x0, x1 = x_range.start, x_range.end
        y0, y1 = y_range.start, y_range.end
        if isinstance(xaxis, DatetimeAxis):
            x = convert_timestamp(x)
            if isinstance(x0, (float, int)):
                x0 = convert_timestamp(x0)
            if isinstance(x1, (float, int)):
                x1 = convert_timestamp(x1)
        if isinstance(yaxis, DatetimeAxis):
            y = convert_timestamp(y)
            if isinstance(y0, (float, int)):
                y0 = convert_timestamp(y0)
            if isinstance(y1, (float, int)):
                y1 = convert_timestamp(y1)
        xs, ys = {x_range.name: x}, {y_range.name: y}
        xspan, yspan = x1 - x0, y1 - y0
        xfactor, yfactor = (x-x0) / xspan, (y-y0) / yspan

        # Use computed factors to compute x/y position on other axes
        for values, factor, ranges, axis in (
            (xs, xfactor, extra_x, xaxis),
            (ys, yfactor, extra_y, yaxis)
        ):
            for rng in ranges:
                value = rng.start + (rng.end-rng.start) * factor
                if isinstance(axis, DatetimeAxis) and isinstance(value, (float, int)):
                    value = convert_timestamp(value)
                values[rng.name] = value

        return {'xs': xs, 'ys': ys}


class SingleTapCallback(TapCallback):
    """Returns the mouse x/y-position on tap event.

    """

    on_events = ['tap']


class PressUpCallback(TapCallback):
    """Returns the mouse x/y-position of a pressup mouse event.

    """

    on_events = ['pressup']


class PanEndCallback(TapCallback):
    """Returns the mouse x/y-position of a pan end event.

    """

    on_events = ['panend']


class DoubleTapCallback(TapCallback):
    """Returns the mouse x/y-position on doubletap event.

    """

    on_events = ['doubletap']


class MouseEnterCallback(PointerXYCallback):
    """Returns the mouse x/y-position on mouseenter event, i.e. when
    mouse enters the plot canvas.

    """

    on_events = ['mouseenter']


class MouseLeaveCallback(PointerXYCallback):
    """Returns the mouse x/y-position on mouseleave event, i.e. when
    mouse leaves the plot canvas.

    """

    on_events = ['mouseleave']


class RangeXYCallback(Callback):
    """Returns the x/y-axis ranges of a plot.

    """

    debounce = 100

    on_events = ['rangesupdate']

    models = ['plot']

    extra_handles = ['x_range', 'y_range']

    attributes = {
        'x0': 'cb_obj.x0',
        'y0': 'cb_obj.y0',
        'x1': 'cb_obj.x1',
        'y1': 'cb_obj.y1',
    }

    def initialize(self, plot_id=None):
        super().initialize(plot_id)
        for stream in self.streams:
            msg = self._process_msg({})
            stream.update(**msg)

    def _process_msg(self, msg):
        if self.plot.state.x_range is not self.plot.handles['x_range']:
            x_range = self.plot.handles['x_range']
            msg['x0'], msg['x1'] = x_range.start, x_range.end
        if self.plot.state.y_range is not self.plot.handles['y_range']:
            y_range = self.plot.handles['y_range']
            msg['y0'], msg['y1'] = y_range.start, y_range.end
        data = {}
        if 'x0' in msg and 'x1' in msg:
            x0, x1 = msg['x0'], msg['x1']
            if isinstance(self.plot.handles.get('xaxis'), DatetimeAxis):
                if not isinstance(x0, datetime_types):
                    x0 = convert_timestamp(x0)
                if not isinstance(x1, datetime_types):
                    x1 = convert_timestamp(x1)
            if x0 > x1:
                x0, x1 = x1, x0
            data['x_range'] = (x0, x1)
        if 'y0' in msg and 'y1' in msg:
            y0, y1 = msg['y0'], msg['y1']
            if isinstance(self.plot.handles.get('yaxis'), DatetimeAxis):
                if not isinstance(y0, datetime_types):
                    y0 = convert_timestamp(y0)
                if not isinstance(y1, datetime_types):
                    y1 = convert_timestamp(y1)
            if y0 > y1:
                y0, y1 = y1, y0
            data['y_range'] = (y0, y1)
        return self._transform(data)


class RangeXCallback(RangeXYCallback):
    """Returns the x-axis range of a plot.

    """

    on_events = ['rangesupdate']

    models = ['plot']

    attributes = {
        'x0': 'cb_obj.x0',
        'x1': 'cb_obj.x1',
    }


class RangeYCallback(RangeXYCallback):
    """Returns the y-axis range of a plot.

    """

    on_events = ['rangesupdate']

    models = ['plot']

    attributes = {
        'y0': 'cb_obj.y0',
        'y1': 'cb_obj.y1'
    }


class PlotSizeCallback(Callback):
    """Returns the actual width and height of a plot once the layout
    solver has executed.

    """

    models = ['plot']
    attributes = {'width': 'cb_obj.inner_width',
                  'height': 'cb_obj.inner_height'}
    on_changes = ['inner_width', 'inner_height']

    debounce = 100

    def _process_msg(self, msg):
        if msg.get('width') and msg.get('height'):
            return self._transform(msg)
        else:
            return {}


class SelectModeCallback(Callback):

    attributes = {'box_mode': 'box_select.mode',
                  'lasso_mode': 'lasso_select.mode'}
    models = ['box_select', 'lasso_select']
    on_changes = ['mode']

    def _process_msg(self, msg):
        stream = self.streams[0]
        if 'box_mode' in msg:
            mode = msg.pop('box_mode')
            if mode != stream.mode:
                msg['mode'] = mode
        if 'lasso_mode' in msg:
            mode = msg.pop('lasso_mode')
            if mode != stream.mode:
                msg['mode'] = mode
        return msg


class BoundsCallback(PopupMixin, Callback):
    """Returns the bounds of a box_select tool.

    """

    attributes = {'x0': 'cb_obj.geometry.x0',
                  'x1': 'cb_obj.geometry.x1',
                  'y0': 'cb_obj.geometry.y0',
                  'y1': 'cb_obj.geometry.y1'}
    geom_type = 'rect'
    models = ['plot']
    on_events = ['selectiongeometry']

    skip_events = [lambda event: event.geometry['type'] != 'rect',
                   lambda event: not event.final]

    def _process_msg(self, msg):
        if all(c in msg for c in ['x0', 'y0', 'x1', 'y1']):
            if isinstance(self.plot.handles.get('xaxis'), DatetimeAxis):
                msg['x0'] = convert_timestamp(msg['x0'])
                msg['x1'] = convert_timestamp(msg['x1'])
            if isinstance(self.plot.handles.get('yaxis'), DatetimeAxis):
                msg['y0'] = convert_timestamp(msg['y0'])
                msg['y1'] = convert_timestamp(msg['y1'])
            msg = {'bounds': (msg['x0'], msg['y0'], msg['x1'], msg['y1'])}
            return self._transform(msg)
        else:
            return {}


class SelectionXYCallback(BoundsCallback):
    """Converts a bounds selection to numeric or categorical x-range
    and y-range selections.

    """

    def _process_msg(self, msg):
        msg = super()._process_msg(msg)
        if 'bounds' not in msg:
            return msg
        el = self.plot.current_frame
        x0, y0, x1, y1 = msg['bounds']
        x_range = self.plot.handles['x_range']
        if isinstance(x_range, FactorRange):
            x0, x1 = round(x0), round(x1)
            xfactors = x_range.factors[x0: x1]
            if x_range.tags and x_range.tags[0]:
                xdim = el.get_dimension(x_range.tags[0][0][0])
                if xdim and hasattr(el, 'interface'):
                    dtype = el.interface.dtype(el, xdim)
                    try:
                        xfactors = list(np.array(xfactors).astype(dtype))
                    except Exception:
                        pass
            msg['x_selection'] = xfactors
        else:
            msg['x_selection'] = (x0, x1)
        y_range = self.plot.handles['y_range']
        if isinstance(y_range, FactorRange):
            y0, y1 = round(y0), round(y1)
            yfactors = y_range.factors[y0: y1]
            if y_range.tags and y_range.tags[0]:
                ydim = el.get_dimension(y_range.tags[0][0][0])
                if ydim and hasattr(el, 'interface'):
                    dtype = el.interface.dtype(el, ydim)
                    try:
                        yfactors = list(np.array(yfactors).astype(dtype))
                    except Exception:
                        pass
            msg['y_selection'] = yfactors
        else:
            msg['y_selection'] = (y0, y1)
        return msg


class BoundsXCallback(Callback):
    """Returns the bounds of a xbox_select tool.

    """

    attributes = {'x0': 'cb_obj.geometry.x0', 'x1': 'cb_obj.geometry.x1'}
    models = ['plot']
    on_events = ['selectiongeometry']

    skip_events = [lambda event: event.geometry['type'] != 'rect',
                   lambda event: not event.final]

    def _process_msg(self, msg):
        if all(c in msg for c in ['x0', 'x1']):
            if isinstance(self.plot.handles.get('xaxis'), DatetimeAxis):
                msg['x0'] = convert_timestamp(msg['x0'])
                msg['x1'] = convert_timestamp(msg['x1'])
            msg = {'boundsx': (msg['x0'], msg['x1'])}
            return self._transform(msg)
        else:
            return {}


class BoundsYCallback(Callback):
    """Returns the bounds of a ybox_select tool.

    """

    attributes = {'y0': 'cb_obj.geometry.y0', 'y1': 'cb_obj.geometry.y1'}
    models = ['plot']
    on_events = ['selectiongeometry']

    skip_events = [lambda event: event.geometry['type'] != 'rect',
                   lambda event: not event.final]

    def _process_msg(self, msg):
        if all(c in msg for c in ['y0', 'y1']):
            if isinstance(self.plot.handles.get('yaxis'), DatetimeAxis):
                msg['y0'] = convert_timestamp(msg['y0'])
                msg['y1'] = convert_timestamp(msg['y1'])
            msg = {'boundsy': (msg['y0'], msg['y1'])}
            return self._transform(msg)
        else:
            return {}


class LassoCallback(PopupMixin, Callback):

    attributes = {'xs': 'cb_obj.geometry.x', 'ys': 'cb_obj.geometry.y'}
    geom_type = 'poly'
    models = ['plot']
    on_events = ['selectiongeometry']

    skip_events = [lambda event: event.geometry['type'] != 'poly',
                   lambda event: not event.final]

    def _process_msg(self, msg):
        if not all(c in msg for c in ('xs', 'ys')):
            return {}
        xs, ys = msg['xs'], msg['ys']
        if isinstance(xs, dict):
            xs = ((int(i), x) for i, x in xs.items())
            xs = [x for _, x in sorted(xs)]
        if isinstance(ys, dict):
            ys = ((int(i), y) for i, y in ys.items())
            ys = [y for _, y in sorted(ys)]
        if xs is None or ys is None:
            return {}
        return {'geometry': np.column_stack([xs, ys])}


class Selection1DCallback(PopupMixin, Callback):
    """Returns the current selection on a ColumnDataSource.

    """

    attributes = {'index': 'cb_obj.indices'}
    models = ['selected']
    on_changes = ['indices']

    def _watch_position(self):
        self.plot.state.on_event('selectiongeometry', self._update_selection_event)
        source = self.plot.handles['source']
        renderer = self.plot.handles['glyph_renderer']
        selected = self.plot.handles['selected']

        self.plot.state.js_on_event('selectiongeometry', CustomJS(
            args=dict(panel=self._panel, renderer=renderer, source=source, selected=selected, popup_position=self._popup_position),
            code="""
            export default ({panel, renderer, source, selected, popup_position}, cb_obj, _) => {
                panel.visible = false;  // Hide the popup panel so it doesn't show in previous location
                const el = panel.elements[1];
                if ((el && !el.visible) || !cb_obj.final) {
                    return;
                }
                let x, y, xs, ys;
                let indices = selected.indices;
                if (cb_obj.geometry.type == 'point') {
                    indices = indices.slice(-1);
                }

                if (renderer.glyph.x && renderer.glyph.y) {
                    xs = source.get_column(renderer.glyph.x.field);
                    ys = source.get_column(renderer.glyph.y.field);
                } else if (renderer.glyph.right && renderer.glyph.top) {
                    xs = source.get_column(renderer.glyph.right.field);
                    ys = source.get_column(renderer.glyph.top.field);
                } else if (renderer.glyph.x1 && renderer.glyph.y1) {
                    xs = source.get_column(renderer.glyph.x1.field);
                    ys = source.get_column(renderer.glyph.y1.field);
                } else if (renderer.glyph.xs && renderer.glyph.ys) {
                    xs = source.get_column(renderer.glyph.xs.field);
                    ys = source.get_column(renderer.glyph.ys.field);
                }

                if (!xs || !ys || !indices.length) {
                    return;
                }

                let minX, maxX, minY, maxY;

                // Loop over each index in the selection and find the corresponding polygon coordinates
                for (const i of indices) {
                    let ix = xs[i];
                    let iy = ys[i];
                    let tx, ty;

                    // Check if the values are numbers or nested arrays
                    if (typeof ix === 'number') {
                        tx = ix;
                        ty = iy;
                    } else {
                        // Drill down into nested arrays until we find the number values
                        while (ix.length && typeof ix[0] !== 'number') {
                            ix = ix[0];
                            iy = iy[0];
                        }

                        // Set tx and ty based on the popup position preferences
                        if (popup_position.includes('left')) {
                            tx = Math.min(...ix);
                        } else if (popup_position.includes('right')) {
                            tx = Math.max(...ix);
                        } else {
                            tx = (Math.min(...ix) + Math.max(...ix)) / 2;
                        }

                        if (popup_position.includes('top')) {
                            ty = Math.max(...iy);
                        } else if (popup_position.includes('bottom')) {
                            ty = Math.min(...iy);
                        } else {
                            ty = (Math.min(...iy) + Math.max(...iy)) / 2;
                        }
                    }

                    // Update the min/max values for x and y
                    if (minX === undefined || tx < minX) { minX = tx; }
                    if (maxX === undefined || tx > maxX) { maxX = tx; }
                    if (minY === undefined || ty < minY) { minY = ty; }
                    if (maxY === undefined || ty > maxY) { maxY = ty; }
                }

                // Set x and y based on popup_position preference
                if (popup_position.includes('left')) {
                    x = minX;
                } else if (popup_position.includes('right')) {
                    x = maxX;
                } else {
                    x = (minX + maxX) / 2;
                }

                if (popup_position.includes('top')) {
                    y = maxY;
                } else if (popup_position.includes('bottom')) {
                    y = minY;
                } else {
                    y = (minY + maxY) / 2;
                }

                // Set the popup position and make it visible
                panel.position.setv({x, y});
                panel.visible = true;
            }
            """,
        ))

    def _get_position(self, event):
        el = self.plot.current_frame
        if isinstance(el, Dataset):
            s = self.streams[0]
            sel = el.iloc[s.index]
            # get the most top-right point
            (_, x1), (_, y1) = sel.range(0), sel.range(1)
            return dict(x=x1, y=y1)
        return super()._get_position(event)

    def _process_msg(self, msg):
        el = self.plot.current_frame
        if 'index' in msg:
            msg = {'index': [int(v) for v in msg['index']]}
            if isinstance(el, Table):
                # Ensure that explicitly applied selection does not
                # trigger new events
                sel = el.opts.get('plot').kwargs.get('selected')
                if sel is not None and list(sel) == msg['index']:
                    return {}
            return self._transform(msg)
        else:
            return {}


class ResetCallback(Callback):
    """Signals the Reset stream if an event has been triggered.

    """

    models = ['plot']
    on_events = ['reset']

    def _process_msg(self, msg):
        msg = {'resetting': True}
        return self._transform(msg)


class CDSCallback(Callback):
    """A Stream callback that syncs the data on a bokeh ColumnDataSource
    model with Python.

    """

    attributes = {'data': 'source.data'}
    models = ['source']
    on_changes = ['data', 'patching']

    def initialize(self, plot_id=None):
        super().initialize(plot_id)
        plot = self.plot
        data = self._process_msg({'data': plot.handles['source'].data})['data']
        for stream in self.streams:
            stream.update(data=data)

    def _process_msg(self, msg):
        if 'data' not in msg:
            return {}
        msg['data'] = dict(msg['data'])
        for col, values in msg['data'].items():
            if isinstance(values, dict):
                shape = values.pop('shape', None)
                dtype = values.pop('dtype', None)
                values.pop('dimension', None)
                items = sorted([(int(k), v) for k, v in values.items()])
                values = [v for k, v in items]
                if dtype is not None:
                    values = np.array(values, dtype=dtype).reshape(shape)
            elif isinstance(values, list) and values and isinstance(values[0], dict):
                new_values = []
                for vals in values:
                    if isinstance(vals, dict):
                        shape = vals.pop('shape', None)
                        dtype = vals.pop('dtype', None)
                        vals.pop('dimension', None)
                        vals = sorted([(int(k), v) for k, v in vals.items()])
                        vals = [v for k, v in vals]
                        if dtype is not None:
                            vals = np.array(vals, dtype=dtype).reshape(shape)
                    new_values.append(vals)
                values = new_values
            elif any(isinstance(v, (int, float)) for v in values):
                values = [np.nan if v is None else v for v in values]
            elif (
                isinstance(values, list)
                and len(values) == 4
                and isinstance(values[2], str)
                and values[2] in ("big", "little")
                and isinstance(values[3], list)
            ):
                # Account for issue seen in https://github.com/holoviz/geoviews/issues/584
                # This could be fixed in Bokeh 3.0, but has not been tested.
                # Example:
                # ['pm9vF9dSY8EAAADgPFNjwQAAAMAmU2PBAAAAAMtSY8E=','float64', 'little', [4]]
                buffer = base64.decodebytes(values[0].encode())
                dtype = np.dtype(values[1]).newbyteorder(values[2])
                values = np.frombuffer(buffer, dtype)
            msg['data'][col] = values
        return self._transform(msg)


class GlyphDrawCallback(CDSCallback):

    _style_callback = """
      var types = Bokeh.require("core/util/types");
      var changed = false
      for (var i = 0; i < cb_obj.length; i++) {
        for (var style in styles) {
          var value = styles[style];
          if (types.isArray(value)) {
            value = value[i % value.length];
          }
          if (cb_obj.data[style][i] !== value) {
            cb_obj.data[style][i] = value;
            changed = true;
          }
        }
      }
      if (changed)
        cb_obj.change.emit()
    """

    def _create_style_callback(self, cds, glyph):
        stream = self.streams[0]
        col = cds.column_names[0]
        length = len(cds.data[col])
        for style, values in stream.styles.items():
            cds.data[style] = [values[i % len(values)] for i in range(length)]
            setattr(glyph, style, style)
        cb = CustomJS(code=self._style_callback,
                      args={'styles': stream.styles,
                            'empty': stream.empty_value})
        cds.js_on_change('data', cb)

    def _update_cds_vdims(self, data):
        """Add any value dimensions not already in the data ensuring the
        element can be reconstituted in entirety.

        """
        element = self.plot.current_frame
        stream = self.streams[0]
        for d in element.vdims:
            dim = dimension_sanitizer(d.name)
            if dim in data:
                continue
            values = element.dimension_values(d)
            if len(values) != len(next(iter(data.values()))):
                values = np.concatenate([values, [stream.empty_value]])
            data[dim] = values


class PointDrawCallback(GlyphDrawCallback):

    def initialize(self, plot_id=None):
        plot = self.plot
        stream = self.streams[0]
        cds = plot.handles['source']
        glyph = plot.handles['glyph']
        renderers = [plot.handles['glyph_renderer']]
        kwargs = {}
        if stream.num_objects:
            kwargs['num_objects'] = stream.num_objects
        if stream.tooltip:
            kwargs['description'] = stream.tooltip
        if stream.styles:
            self._create_style_callback(cds, glyph)
        if stream.empty_value is not None:
            kwargs['empty_value'] = stream.empty_value
        point_tool = PointDrawTool(
            add=all(s.add for s in self.streams),
            drag=all(s.drag for s in self.streams),
            renderers=renderers, **kwargs)
        self.plot.state.tools.append(point_tool)
        self._update_cds_vdims(cds.data)
        # Add any value dimensions not already in the CDS data
        # ensuring the element can be reconstituted in entirety
        super().initialize(plot_id)

    def _process_msg(self, msg):
        self._update_cds_vdims(msg['data'])
        return super()._process_msg(msg)


class CurveEditCallback(GlyphDrawCallback):

    def initialize(self, plot_id=None):
        plot = self.plot
        stream = self.streams[0]
        cds = plot.handles['cds']
        glyph = plot.handles['glyph']
        renderer = plot.state.scatter(glyph.x, glyph.y, source=cds,
                                      visible=False, **stream.style)
        renderers = [renderer]
        kwargs = {}
        if stream.tooltip:
            kwargs['description'] = stream.tooltip
        point_tool = PointDrawTool(
            add=False, drag=True, renderers=renderers, **kwargs
        )
        code="renderer.visible = tool.active || (cds.selected.indices.length > 0)"
        show_vertices = CustomJS(args={'renderer': renderer, 'cds': cds, 'tool': point_tool}, code=code)
        point_tool.js_on_change('change:active', show_vertices)
        cds.selected.js_on_change('indices', show_vertices)

        self.plot.state.tools.append(point_tool)
        self._update_cds_vdims(cds.data)
        super().initialize(plot_id)

    def _process_msg(self, msg):
        self._update_cds_vdims(msg['data'])
        return super()._process_msg(msg)

    def _update_cds_vdims(self, data):
        """Add any value dimensions not already in the data ensuring the
        element can be reconstituted in entirety.

        """
        element = self.plot.current_frame
        for d in element.vdims:
            dim = dimension_sanitizer(d.name)
            if dim not in data:
                data[dim] = element.dimension_values(d)


class PolyDrawCallback(GlyphDrawCallback):

    def initialize(self, plot_id=None):
        plot = self.plot
        stream = self.streams[0]
        cds = self.plot.handles['cds']
        glyph = self.plot.handles['glyph']
        renderers = [plot.handles['glyph_renderer']]
        kwargs = {}
        if stream.num_objects:
            kwargs['num_objects'] = stream.num_objects
        if stream.show_vertices:
            vertex_style = dict({'size': 10}, **stream.vertex_style)
            r1 = plot.state.scatter([], [], **vertex_style)
            kwargs['vertex_renderer'] = r1
        if stream.styles:
            self._create_style_callback(cds, glyph)
        if stream.tooltip:
            kwargs['description'] = stream.tooltip
        if stream.empty_value is not None:
            kwargs['empty_value'] = stream.empty_value
        poly_tool = PolyDrawTool(
            drag=all(s.drag for s in self.streams), renderers=renderers,
            **kwargs
        )
        plot.state.tools.append(poly_tool)
        self._update_cds_vdims(cds.data)
        super().initialize(plot_id)

    def _process_msg(self, msg):
        self._update_cds_vdims(msg['data'])
        return super()._process_msg(msg)

    def _update_cds_vdims(self, data):
        """Add any value dimensions not already in the data ensuring the
        element can be reconstituted in entirety.

        """
        element = self.plot.current_frame
        stream = self.streams[0]
        interface = element.interface
        scalar_kwargs = {'per_geom': True} if interface.multi else {}
        for d in element.vdims:
            scalar = element.interface.isunique(element, d, **scalar_kwargs)
            dim = dimension_sanitizer(d.name)
            if dim not in data:
                if scalar:
                    values = element.dimension_values(d, not scalar)
                else:
                    values = [arr[:, 0] for arr in element.split(datatype='array', dimensions=[dim])]
                if len(values) != len(data['xs']):
                    values = np.concatenate([values, [stream.empty_value]])
                data[dim] = values


class FreehandDrawCallback(PolyDrawCallback):

    def initialize(self, plot_id=None):
        plot = self.plot
        cds = plot.handles['cds']
        glyph = plot.handles['glyph']
        stream = self.streams[0]
        if stream.styles:
            self._create_style_callback(cds, glyph)
        kwargs = {}
        if stream.tooltip:
            kwargs['description'] = stream.tooltip
        if stream.empty_value is not None:
            kwargs['empty_value'] = stream.empty_value
        poly_tool = FreehandDrawTool(
            num_objects=stream.num_objects,
            renderers=[plot.handles['glyph_renderer']],
            **kwargs
        )
        plot.state.tools.append(poly_tool)
        self._update_cds_vdims(cds.data)
        CDSCallback.initialize(self, plot_id)


class BoxEditCallback(GlyphDrawCallback):

    attributes = {'data': 'cds.data'}
    models = ['cds']

    def _path_initialize(self):
        plot = self.plot
        cds = plot.handles['cds']
        data = cds.data
        element = self.plot.current_frame

        l, b, r, t =  [], [], [], []
        for x, y in zip(data['xs'], data['ys'], strict=None):
            x0, x1 = (np.nanmin(x), np.nanmax(x))
            y0, y1 = (np.nanmin(y), np.nanmax(y))
            l.append(x0)
            b.append(y0)
            r.append(x1)
            t.append(y1)
        data = {'left': l, 'bottom': b, 'right': r, 'top': t}
        data.update({vd.name: element.dimension_values(vd, expanded=False) for vd in element.vdims})
        cds.data.update(data)
        style = self.plot.style[self.plot.cyclic_index]
        style.pop('cmap', None)
        r1 = plot.state.quad(left='left', bottom='bottom', right='right', top='top', source=cds, **style)
        if plot.handles['glyph_renderer'] in self.plot.state.renderers:
            self.plot.state.renderers.remove(plot.handles['glyph_renderer'])
        data = self._process_msg({'data': data})['data']
        for stream in self.streams:
            stream.update(data=data)
        return r1

    def initialize(self, plot_id=None):
        from .path import PathPlot

        stream = self.streams[0]
        cds = self.plot.handles['cds']

        kwargs = {}
        if stream.num_objects:
            kwargs['num_objects'] = stream.num_objects
        if stream.tooltip:
            kwargs['description'] = stream.tooltip

        renderer = self.plot.handles['glyph_renderer']
        if isinstance(self.plot, PathPlot):
            renderer = self._path_initialize()
        if stream.styles:
            self._create_style_callback(cds, renderer.glyph)
        if BOKEH_GE_3_3_0:
            # First version with Quad support
            box_tool = BoxEditTool(renderers=[renderer], **kwargs)
            self.plot.state.tools.append(box_tool)
        else:
            warn("BoxEditTool requires Bokeh >= 3.3")
        self._update_cds_vdims(cds.data)
        super(CDSCallback, self).initialize()

    def _process_msg(self, msg):
        data = super()._process_msg(msg)
        if 'data' not in data:
            return {}
        values = dict(data['data'])
        values['x0'] = values.pop("left")
        values['y0'] = values.pop("bottom")
        values['x1'] = values.pop("right")
        values['y1'] = values.pop("top")
        msg = {'data': values}
        self._update_cds_vdims(msg['data'])
        return self._transform(msg)


class PolyEditCallback(PolyDrawCallback):

    def initialize(self, plot_id=None):
        plot = self.plot
        cds = plot.handles['cds']
        vertex_tool = None
        if all(s.shared for s in self.streams):
            tools = [tool for tool in plot.state.tools if isinstance(tool, PolyEditTool)]
            vertex_tool = tools[0] if tools else None

        stream = self.streams[0]
        kwargs = {}
        if stream.tooltip:
            kwargs['description'] = stream.tooltip
        if vertex_tool is None:
            vertex_style = dict({'size': 10}, **stream.vertex_style)
            r1 = plot.state.scatter([], [], **vertex_style)
            vertex_tool = PolyEditTool(vertex_renderer=r1, **kwargs)
            plot.state.tools.append(vertex_tool)
        vertex_tool.renderers.append(plot.handles['glyph_renderer'])
        self._update_cds_vdims(cds.data)
        CDSCallback.initialize(self, plot_id)


Stream._callbacks['bokeh'].update({
    PointerXY   : PointerXYCallback,
    PointerX    : PointerXCallback,
    PointerY    : PointerYCallback,
    Tap         : TapCallback,
    SingleTap   : SingleTapCallback,
    DoubleTap   : DoubleTapCallback,
    PressUp     : PressUpCallback,
    PanEnd      : PanEndCallback,
    MouseEnter  : MouseEnterCallback,
    MouseLeave  : MouseLeaveCallback,
    RangeXY     : RangeXYCallback,
    RangeX      : RangeXCallback,
    RangeY      : RangeYCallback,
    BoundsXY    : BoundsCallback,
    BoundsX     : BoundsXCallback,
    BoundsY     : BoundsYCallback,
    Lasso       : LassoCallback,
    Selection1D : Selection1DCallback,
    PlotSize    : PlotSizeCallback,
    SelectionXY : SelectionXYCallback,
    Draw        : DrawCallback,
    PlotReset   : ResetCallback,
    CDSStream   : CDSCallback,
    BoxEdit     : BoxEditCallback,
    PointDraw   : PointDrawCallback,
    CurveEdit   : CurveEditCallback,
    FreehandDraw: FreehandDrawCallback,
    PolyDraw    : PolyDrawCallback,
    PolyEdit    : PolyEditCallback,
    SelectMode  : SelectModeCallback,
    MultiAxisTap: MultiAxisTapCallback
})

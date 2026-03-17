# standard library imports
import base64
import copy
import pickle
import uuid
from collections import namedtuple

from dash.exceptions import PreventUpdate

# Holoviews imports
import holoviews as hv
from holoviews.core.decollate import (
    expr_to_fn_of_stream_contents,
    initialize_dynamic,
    to_expr_extract_streams,
)
from holoviews.plotting.plotly import DynamicMap, PlotlyRenderer
from holoviews.plotting.plotly.callbacks import (
    BoundsXCallback,
    BoundsXYCallback,
    BoundsYCallback,
    RangeXCallback,
    RangeXYCallback,
    RangeYCallback,
    Selection1DCallback,
)
from holoviews.plotting.plotly.util import clean_internal_figure_properties
from holoviews.streams import Derived, History

# Dash imports
try:
    from dash import dcc, html
except ImportError:
    import dash_core_components as dcc
    import dash_html_components as html
# plotly.py imports
import plotly.graph_objects as go
from dash import callback_context
from dash.dependencies import Input, Output, State

# Activate plotly as current HoloViews extension
hv.extension("plotly")


# Named tuples definitions
StreamCallback = namedtuple("StreamCallback", ["input_ids", "fn", "output_id"])
DashComponents = namedtuple(
    "DashComponents", ["graphs", "kdims", "store", "resets", "children"]
)
HoloViewsFunctionSpec = namedtuple("HoloViewsFunctionSpec", ["fn", "kdims", "streams"])


def get_layout_ranges(plot):
    layout_ranges = {}
    fig_dict = plot.state
    for k in fig_dict['layout']:
        if k.startswith(("xaxis", "yaxis")):
            if "range" in fig_dict['layout'][k]:
                layout_ranges[k] = {"range": fig_dict['layout'][k]["range"]}

        if k.startswith('mapbox'):
            mapbox_ranges = {}
            if "center" in fig_dict['layout'][k]:
                mapbox_ranges["center"] = fig_dict['layout'][k]["center"]
            if "zoom" in fig_dict['layout'][k]:
                mapbox_ranges["zoom"] = fig_dict['layout'][k]["zoom"]
            if mapbox_ranges:
                layout_ranges[k] = mapbox_ranges

    return layout_ranges


def plot_to_figure(
        plot, reset_nclicks=0, layout_ranges=None, responsive=True, use_ranges=True
):
    """Convert a HoloViews plotly plot to a plotly.py Figure.

    Parameters
    ----------
    plot : A HoloViews plotly plot object
    reset_nclicks : int
        Number of times a reset button associated with the plot has been
        clicked

    Returns
    -------
    A plotly.py Figure
    """
    fig_dict = plot.state
    clean_internal_figure_properties(fig_dict)

    # Enable uirevision to preserve user-interaction state
    # Don't use reset_nclicks directly because 0 is treated as no revision
    fig_dict['layout']['uirevision'] = "reset-" + str(reset_nclicks)

    # Remove range specification so plotly.js autorange + uirevision is in control
    if layout_ranges and use_ranges:
        for k in fig_dict['layout']:
            if k.startswith(("xaxis", "yaxis")):
                fig_dict['layout'][k].pop('range', None)
            if k.startswith('mapbox'):
                fig_dict['layout'][k].pop('zoom', None)
                fig_dict['layout'][k].pop('center', None)

    # Remove figure width height, let container decide
    if responsive:
        fig_dict['layout'].pop('autosize', None)

    if responsive is True or responsive == "width":
        fig_dict['layout'].pop('width', None)

    if responsive is True or responsive == "height":
        fig_dict['layout'].pop('height', None)

    # Pass to figure constructor to expand magic underscore notation
    fig = go.Figure(fig_dict)

    if layout_ranges and use_ranges:
        fig.update_layout(layout_ranges)

    return fig


def to_function_spec(hvobj):
    """Convert Dynamic HoloViews object into a pure function that accepts kdim values
    and stream contents as positional arguments.

    This borrows the low-level holoviews decollate logic, but instead of returning
    DynamicMap with cloned streams, returns a HoloViewsFunctionSpec.

    Parameters
    ----------
    hvobj : A potentially dynamic Holoviews object

    Returns
    -------
    HoloViewsFunctionSpec
    """
    kdims_list = []
    original_streams = []
    streams = []
    stream_mapping = {}
    initialize_dynamic(hvobj)
    expr = to_expr_extract_streams(
        hvobj, kdims_list, streams, original_streams, stream_mapping
    )
    expr_fn = expr_to_fn_of_stream_contents(expr, nkdims=len(kdims_list))

    # Check for unbounded dimensions
    if isinstance(hvobj, DynamicMap) and hvobj.unbounded:
        dims = ', '.join(f'{dim!r}' for dim in hvobj.unbounded)
        msg = ('DynamicMap cannot be displayed without explicit indexing '
               'as {dims} dimension(s) are unbounded. '
               '\nSet dimensions bounds with the DynamicMap redim.range '
               'or redim.values methods.')
        raise ValueError(msg.format(dims=dims))

    # Build mapping from kdims to values/range
    dimensions_dict = {d.name: d for d in hvobj.dimensions()}
    kdims = {}
    for k in kdims_list:
        dim = dimensions_dict[k.name]
        label = dim.label or dim.name
        kdims[k.name] = label, dim.values or dim.range

    return HoloViewsFunctionSpec(fn=expr_fn, kdims=kdims, streams=original_streams)


def populate_store_with_stream_contents(
        store_data, streams
):
    """Add contents of streams to the store dictionary

    Parameters
    ----------
    store_data
        The store dictionary
    streams
        List of streams whose contents should be added to the store

    Returns
    -------
    None
    """
    for stream in streams:
        # Add stream
        store_data["streams"][id(stream)] = copy.deepcopy(stream.contents)
        if isinstance(stream, Derived):
            populate_store_with_stream_contents(store_data, stream.input_streams)
        elif isinstance(stream, History):
            populate_store_with_stream_contents(store_data, [stream.input_stream])


def build_derived_callback(derived_stream):
    """Build StreamCallback for Derived stream

    Parameters
    ----------
    derived_stream
        A Derived stream

    Returns
    -------
    StreamCallback
    """
    input_ids = [id(stream) for stream in derived_stream.input_streams]
    constants = copy.copy(derived_stream.constants)
    transform = derived_stream.transform_function

    def derived_callback(*stream_values):
        return transform(stream_values=stream_values, constants=constants)

    return StreamCallback(
        input_ids=input_ids, fn=derived_callback, output_id=id(derived_stream)
    )


def build_history_callback(history_stream):
    """Build StreamCallback for History stream

    Parameters
    ----------
    history_stream
        A History stream

    Returns
    -------
    StreamCallback
    """
    history_id = id(history_stream)
    input_stream_id = id(history_stream.input_stream)

    def history_callback(prior_value, input_value):
        new_value = copy.deepcopy(prior_value)
        new_value["values"].append(input_value)
        return new_value

    return StreamCallback(
        input_ids=[history_id, input_stream_id],
        fn=history_callback,
        output_id=history_id
    )


def populate_stream_callback_graph(stream_callbacks, streams):
    """Populate the stream_callbacks dict with StreamCallback instances
    associated with all of the History and Derived streams in input stream list.

    Input streams to any History or Derived streams are processed recursively

    Parameters
    ----------
    stream_callbacks
        dict from id(stream) to StreamCallbacks that should
        be populated.
        Order will be a breadth-first traversal of the provided
        streams list, and any input streams that these depend on.

    streams
        List of streams to build StreamCallbacks from

    Returns
    -------
    None
    """
    for stream in streams:
        if isinstance(stream, Derived):
            cb = build_derived_callback(stream)
            if cb.output_id not in stream_callbacks:
                stream_callbacks[cb.output_id] = cb
                populate_stream_callback_graph(stream_callbacks, stream.input_streams)
        elif isinstance(stream, History):
            cb = build_history_callback(stream)
            if cb.output_id not in stream_callbacks:
                stream_callbacks[cb.output_id] = cb
                populate_stream_callback_graph(stream_callbacks, [stream.input_stream])


def encode_store_data(store_data):
    """Encode store_data dict into a JSON serializable dict

    This is currently done by pickling store_data and converting to a base64 encoded
    string. If HoloViews supports JSON serialization in the future, this method could
    be updated to use this approach instead

    Parameters
    ----------
    store_data : dict potentially containing HoloViews objects

    Returns
    -------
    dict that can be JSON serialized
    """
    return {"pickled": base64.b64encode(pickle.dumps(store_data)).decode("utf-8")}


def decode_store_data(store_data):
    """Decode a dict that was encoded by the encode_store_data function.

    Parameters
    ----------
    store_data : dict that was encoded by encode_store_data

    Returns
    -------
    decoded dict
    """
    return pickle.loads(base64.b64decode(store_data["pickled"]))


def to_dash(
        app, hvobjs, reset_button=False, graph_class=dcc.Graph,
        button_class=html.Button, responsive="width", use_ranges=True,
):
    """Build Dash components and callbacks from a collection of HoloViews objects

    Parameters
    ----------
    app : dash.Dash application instance
    hvobjs
        List of HoloViews objects to build Dash components from
    reset_button : bool
        If True, construct a Button component that, when clicked, will
        reset the interactive stream values associated with the provided HoloViews
        objects to their initial values. Defaults to False.
    graph_class
        Class to use when creating Graph components, one of dcc.Graph
        (default) or ddk.Graph.
    button_class
        Class to use when creating reset button component.
        E.g. html.Button (default) or dbc.Button
    responsive : bool, str
        If True graphs will fill their containers width and height
        responsively. If False, graphs will have a fixed size matching their
        HoloViews size. If "width" (default), the width is responsive but
        height matches the HoloViews size. If "height", the height is responsive
        but the width matches the HoloViews size.
    use_ranges : bool
        If True, initialize graphs with the dimension ranges specified
        in the HoloViews objects. If False, allow Dash to perform its own
        auto-range calculations.

    Returns
    -------
    DashComponents named tuple with properties:
        - graphs: List of graph components (with type matching the input
            graph_class argument) with order corresponding to the order
            of the input hvobjs list.
        - resets: List of reset buttons that can be used to reset figure state.
            List has length 1 if reset_button=True and is empty if
            reset_button=False.
        - kdims: Dict from kdim names to Dash Components that can be used to
            set the corresponding kdim value.
        - store: dcc.Store the must be included in the app layout
        - children: Single list of all components above. The order is graphs,
            kdims, resets, and then the store.
    """
    # Number of figures
    num_figs = len(hvobjs)

    # Initialize component properties
    reset_components = []
    graph_components = []
    kdim_components = {}

    # Initialize inputs / outputs / states list
    outputs = []
    inputs = []
    states = []

    # Initialize other
    plots = []
    graph_ids = []
    initial_fig_dicts = []
    all_kdims = {}
    kdims_per_fig = []

    # Initialize stream mappings
    uid_to_stream_ids = {}
    fig_to_fn_stream = {}
    fig_to_fn_stream_ids = {}

    # Plotly stream types
    plotly_stream_types = [
        RangeXYCallback, RangeXCallback, RangeYCallback, Selection1DCallback,
        BoundsXYCallback, BoundsXCallback, BoundsYCallback
    ]

    # Layout ranges
    layout_ranges = []

    for i, hvobj in enumerate(hvobjs):

        fn_spec = to_function_spec(hvobj)

        fig_to_fn_stream[i] = fn_spec
        kdims_per_fig.append(list(fn_spec.kdims))
        all_kdims.update(fn_spec.kdims)

        # Convert to figure once so that we can map streams to axes
        plot = PlotlyRenderer.get_plot(hvobj)
        plots.append(plot)

        layout_ranges.append(get_layout_ranges(plot))
        fig = plot_to_figure(
            plot, reset_nclicks=0, layout_ranges=layout_ranges[-1],
            responsive=responsive, use_ranges=use_ranges,
        ).to_dict()
        initial_fig_dicts.append(fig)

        # Build graphs
        graph_id = 'graph-' + str(uuid.uuid4())
        graph_ids.append(graph_id)
        graph = graph_class(
            id=graph_id,
            figure=fig,
            config={"scrollZoom": True},
        )
        graph_components.append(graph)

        # Build dict from trace uid to plotly callback object
        plotly_streams = {}
        for plotly_stream_type in plotly_stream_types:
            for t in fig["data"]:
                if t.get("uid", None) in plotly_stream_type.instances:
                    plotly_streams.setdefault(plotly_stream_type, {})[t["uid"]] = \
                        plotly_stream_type.instances[t["uid"]]

        # Build dict from trace uid to list of connected HoloViews streams
        for plotly_stream_type, streams_for_type in plotly_streams.items():
            for uid, cb in streams_for_type.items():
                uid_to_stream_ids.setdefault(
                    plotly_stream_type, {}
                ).setdefault(uid, []).extend(
                    [id(stream) for stream in cb.streams]
                )

        outputs.append(Output(component_id=graph_id, component_property='figure'))
        inputs.extend([
            Input(component_id=graph_id, component_property='selectedData'),
            Input(component_id=graph_id, component_property='relayoutData')
        ])

    # Build Store and State list
    store_data = {"streams": {}}
    store_id = 'store-' + str(uuid.uuid4())
    states.append(State(store_id, 'data'))

    # Store holds mapping from id(stream) -> stream.contents for:
    #   - All extracted streams (including derived)
    #   - All input streams for History and Derived streams.
    for fn_spec in fig_to_fn_stream.values():
        populate_store_with_stream_contents(store_data, fn_spec.streams)

    # Initialize empty list of (input_ids, output_id, fn) triples. For each
    #    Derived/History stream, prepend list with triple. Process in
    #    breadth-first order so all inputs to a triple are guaranteed to be earlier
    #    in the list. History streams will input and output their own id, which is
    #    fine.
    stream_callbacks = {}
    for fn_spec in fig_to_fn_stream.values():
        populate_stream_callback_graph(stream_callbacks, fn_spec.streams)

    # For each Figure function, save off list of ids for the streams whose contents
    #    should be passed to the function.
    for i, fn_spec in fig_to_fn_stream.items():
        fig_to_fn_stream_ids[i] = fn_spec.fn, [id(stream) for stream in fn_spec.streams]

    # Add store output
    store = dcc.Store(
        id=store_id,
        data=encode_store_data(store_data),
    )
    outputs.append(Output(store_id, 'data'))

    # Save copy of initial stream contents
    initial_stream_contents = copy.deepcopy(store_data["streams"])

    # Add kdim sliders
    kdim_uuids = []
    for kdim_name, (kdim_label, kdim_range) in all_kdims.items():
        slider_uuid = str(uuid.uuid4())
        slider_id = kdim_name + "-" + slider_uuid
        slider_label_id = kdim_name + "-label-" + slider_uuid
        kdim_uuids.append(slider_uuid)

        html_label = html.Label(id=slider_label_id, children=kdim_label)
        if isinstance(kdim_range, list):
            # list of slider values
            slider = html.Div(children=[
                html_label,
                dcc.Slider(
                    id=slider_id,
                    min=kdim_range[0],
                    max=kdim_range[-1],
                    step=None,
                    marks={
                        m: "" for m in kdim_range
                    },
                    value=kdim_range[0]
            )])
        else:
            # Range of slider values
            slider = html.Div(children=[
                html_label,
                dcc.Slider(
                    id=slider_id,
                    min=kdim_range[0],
                    max=kdim_range[-1],
                    step=(kdim_range[-1] - kdim_range[0]) / 11.0,
                    value=kdim_range[0]
            )])
        kdim_components[kdim_name] = slider
        inputs.append(Input(component_id=slider_id, component_property="value"))

    # Add reset button
    if reset_button:
        reset_id = 'reset-' + str(uuid.uuid4())
        reset_button = button_class(id=reset_id, children="Reset")
        inputs.append(Input(
            component_id=reset_id, component_property='n_clicks'
        ))
        reset_components.append(reset_button)

    # Register Graphs/Store callback
    @app.callback(
        outputs, inputs, states
    )
    def update_figure(*args):
        triggered_prop_ids = {entry["prop_id"] for entry in callback_context.triggered}

        # Unpack args
        selected_dicts = [args[j] or {} for j in range(0, num_figs * 2, 2)]
        relayout_dicts = [args[j] or {} for j in range(1, num_figs * 2, 2)]

        # Get store
        any_change = False
        store_data = decode_store_data(args[-1])
        reset_nclicks = 0
        if reset_button:
            reset_nclicks = args[-2] or 0
            prior_reset_nclicks = store_data.get("reset_nclicks", 0)
            if reset_nclicks != prior_reset_nclicks:
                store_data["reset_nclicks"] = reset_nclicks

                # clear stream values
                store_data["streams"] = copy.deepcopy(initial_stream_contents)
                selected_dicts = [None for _ in selected_dicts]
                relayout_dicts = [None for _ in relayout_dicts]
                any_change = True

        # Init store data if needed
        if store_data is None:
            store_data = {"streams": {}}

        # Get kdim values
        store_data.setdefault("kdims", {})
        for i, kdim in zip(
                range(num_figs * 2, num_figs * 2 + len(all_kdims)),
                all_kdims, strict=None
        ):
            if kdim not in store_data["kdims"] or store_data["kdims"][kdim] != args[i]:
                store_data["kdims"][kdim] = args[i]
                any_change = True

        # Update store_data with interactive stream values
        for fig_ind in range(len(initial_fig_dicts)):
            graph_id = graph_ids[fig_ind]
            # plotly_stream_types
            for plotly_stream_type, uid_to_streams_for_type in uid_to_stream_ids.items():
                for panel_prop in plotly_stream_type.callback_properties:
                    if panel_prop == "selected_data":
                        if graph_id + ".selectedData" in triggered_prop_ids:
                            # Only update selectedData values that just changed.
                            # This way we don't save values that may have been cleared
                            # from the store above by the reset button.
                            stream_event_data = plotly_stream_type.get_event_data_from_property_update(
                                panel_prop, selected_dicts[fig_ind], initial_fig_dicts[fig_ind]
                            )
                            any_change = update_stream_values_for_type(
                                store_data, stream_event_data, uid_to_streams_for_type
                            ) or any_change

                    elif panel_prop == "viewport":
                        if graph_id + ".relayoutData" in triggered_prop_ids:
                            stream_event_data = plotly_stream_type.get_event_data_from_property_update(
                                panel_prop, relayout_dicts[fig_ind], initial_fig_dicts[fig_ind]
                            )

                            stream_event_data = {
                                uid: event_data
                                for uid, event_data in stream_event_data.items()
                                if event_data["x_range"] is not None
                                or event_data[ "y_range"] is not None
                            }
                            any_change = update_stream_values_for_type(
                                store_data, stream_event_data, uid_to_streams_for_type
                            ) or any_change
                    elif panel_prop == "relayout_data":
                        if graph_id + ".relayoutData" in triggered_prop_ids:
                            stream_event_data = plotly_stream_type.get_event_data_from_property_update(
                                panel_prop, relayout_dicts[fig_ind], initial_fig_dicts[fig_ind]
                            )
                            any_change = update_stream_values_for_type(
                                store_data, stream_event_data, uid_to_streams_for_type
                            ) or any_change

        if not any_change:
            raise PreventUpdate

        # Update store with derived/history stream values
        for output_id in reversed(stream_callbacks):
            stream_callback = stream_callbacks[output_id]
            input_ids = stream_callback.input_ids
            fn = stream_callback.fn
            output_id = stream_callback.output_id

            input_values = [store_data["streams"][input_id] for input_id in input_ids]
            output_value = fn(*input_values)
            store_data["streams"][output_id] = output_value

        figs = [None] * num_figs
        for fig_ind, (fn, stream_ids) in fig_to_fn_stream_ids.items():
            fig_kdim_values = [store_data["kdims"][kd] for kd in kdims_per_fig[fig_ind]]
            stream_values = [
                store_data["streams"][stream_id] for stream_id in stream_ids
            ]
            hvobj = fn(*(fig_kdim_values + stream_values))
            plot = PlotlyRenderer.get_plot(hvobj)
            fig = plot_to_figure(
                plot, reset_nclicks=reset_nclicks,
                layout_ranges=layout_ranges[fig_ind], responsive=responsive,
                use_ranges=use_ranges,
            ).to_dict()
            figs[fig_ind] = fig

        return [*figs, encode_store_data(store_data)]

    # Register key dimension slider callbacks
    # Install callbacks to update kdim labels based on slider values
    for i, kdim_name in enumerate(all_kdims):
        kdim_label = all_kdims[kdim_name][0]
        kdim_slider_id = kdim_name + "-" + kdim_uuids[i]
        kdim_label_id = kdim_name + "-label-" + kdim_uuids[i]

        @app.callback(
            Output(component_id=kdim_label_id, component_property="children"),
            [Input(component_id=kdim_slider_id, component_property="value")]
        )
        def update_kdim_label(value, kdim_label=kdim_label):
            return f"{kdim_label}: {value:.2f}"

    # Collect Dash components into DashComponents namedtuple
    components = DashComponents(
        graphs=graph_components,
        kdims=kdim_components,
        resets=reset_components,
        store=store,
        children=(
                graph_components +
                list(kdim_components.values()) +
                reset_components +
                [store]
        )
    )

    return components


def update_stream_values_for_type(store_data, stream_event_data, uid_to_streams_for_type):
    """Update the store with values of streams for a single type

    Parameters
    ----------
    store_data
        Current store dictionary
    stream_event_data
        Potential stream data for current plotly event and
        traces in figures
    uid_to_streams_for_type
        Mapping from trace UIDs to HoloViews streams of
        a particular type

    Returns
    -------
    any_change
        Whether any stream value has been updated
    """
    any_change = False
    for uid, event_data in stream_event_data.items():
        if uid in uid_to_streams_for_type:
            for stream_id in uid_to_streams_for_type[uid]:
                if stream_id not in store_data["streams"] or \
                        store_data["streams"][stream_id] != event_data:
                    store_data["streams"][stream_id] = event_data
                    any_change = True
    return any_change

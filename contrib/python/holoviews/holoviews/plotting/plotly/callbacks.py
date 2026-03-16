from weakref import WeakValueDictionary

from ...element import Tiles
from ...streams import (
    BoundsX,
    BoundsXY,
    BoundsY,
    RangeX,
    RangeXY,
    RangeY,
    Selection1D,
    SelectionXY,
    Stream,
)
from .util import PLOTLY_MAP, PLOTLY_SCATTERMAP, _trace_to_subplot


class PlotlyCallbackMetaClass(type):
    """Metaclass for PlotlyCallback classes.

    We want each callback class to keep track of all of the instances of the class.
    Using a meta class here lets us keep the logic for instance tracking in one place.

    """

    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)

        # Create weak-value dictionary to hold instances of the class
        cls.instances = WeakValueDictionary()

    def __call__(cls, *args, **kwargs):
        inst = super().__call__(*args, **kwargs)

        # Store weak reference to the callback instance in the _instances
        # WeakValueDictionary. This will allow instances to be garbage collected and
        # the references will be automatically removed from the collection when this
        # happens.
        cls.instances[inst.plot.trace_uid] = inst

        return inst


class PlotlyCallback(metaclass=PlotlyCallbackMetaClass):

    def __init__(self, plot, streams, source, **params):
        self.plot = plot
        self.streams = streams
        self.source = source
        self.last_event = None

    @classmethod
    def update_streams_from_property_update(cls, property, property_value, fig_dict):
        event_data = cls.get_event_data_from_property_update(
            property, property_value, fig_dict
        )
        streams = []
        for trace_uid, stream_data in event_data.items():
            if trace_uid in cls.instances:
                cb = cls.instances[trace_uid]
                try:
                    unchanged = stream_data == cb.last_event
                except Exception:
                    unchanged = False
                if unchanged:
                    continue
                cb.last_event = stream_data
                for stream in cb.streams:
                    stream.update(**stream_data)
                    streams.append(stream)

        try:
            Stream.trigger(streams)
        except Exception as e:
            raise e

    @classmethod
    def get_event_data_from_property_update(cls, property, property_value, fig_dict):
        raise NotImplementedError


class Selection1DCallback(PlotlyCallback):
    callback_properties = ["selected_data"]

    @classmethod
    def get_event_data_from_property_update(cls, property, selected_data, fig_dict):

        traces = fig_dict.get('data', [])

        # build event data and compute which trace UIDs are eligible
        # Look up callback with UID
        # graph reference and update the streams
        point_inds = {}
        if selected_data:
            for point in selected_data['points']:
                point_inds.setdefault(point['curveNumber'], [])
                point_inds[point['curveNumber']].append(point['pointNumber'])

        event_data = {}
        for trace_ind, trace in enumerate(traces):
            trace_uid = trace.get('uid', None)
            new_index = point_inds.get(trace_ind, [])
            event_data[trace_uid] = dict(index=new_index)

        return event_data


class BoundsCallback(PlotlyCallback):
    callback_properties = ["selected_data"]
    boundsx = False
    boundsy = False

    @classmethod
    def get_event_data_from_property_update(cls, property, selected_data, fig_dict):
        traces = fig_dict.get('data', [])

        # Initialize event data by clearing box selection on everything
        event_data = {}
        for trace in traces:
            trace_uid = trace.get('uid', None)
            if cls.boundsx and cls.boundsy:
                stream_data = dict(bounds=None)
            elif cls.boundsx:
                stream_data = dict(boundsx=None)
            elif cls.boundsy:
                stream_data = dict(boundsy=None)
            else:
                stream_data = {}

            event_data[trace_uid] = stream_data

        range_data = (selected_data or {}).get("range", {})
        cls.update_event_data_xyaxis(range_data, traces, event_data)
        cls.update_event_data_mapbox(range_data, traces, event_data)

        return event_data

    @classmethod
    def update_event_data_xyaxis(cls, range_data, traces, event_data):
        # Process traces
        for trace in traces:
            trace_type = trace.get('type', 'scatter')
            trace_uid = trace.get('uid', None)

            if _trace_to_subplot.get(trace_type, None) != ['xaxis', 'yaxis']:
                continue

            xref = trace.get('xaxis', 'x')
            yref = trace.get('yaxis', 'y')

            if xref in range_data and yref in range_data:
                new_bounds = (
                    range_data[xref][0], range_data[yref][0],
                    range_data[xref][1], range_data[yref][1]
                )

                if cls.boundsx and cls.boundsy:
                    stream_data = dict(bounds=new_bounds)
                elif cls.boundsx:
                    stream_data = dict(boundsx=(new_bounds[0], new_bounds[2]))
                elif cls.boundsy:
                    stream_data = dict(boundsy=(new_bounds[1], new_bounds[3]))
                else:
                    stream_data = {}

                event_data[trace_uid] = stream_data

    @classmethod
    def update_event_data_mapbox(cls, range_data, traces, event_data):
        # Process traces
        for trace in traces:
            trace_type = trace.get('type', 'scatter')
            trace_uid = trace.get('uid', None)

            if _trace_to_subplot.get(trace_type, None) != [PLOTLY_MAP]:
                continue

            mapbox_ref = trace.get('subplot', PLOTLY_MAP)
            if mapbox_ref in range_data:
                lon_bounds = [range_data[mapbox_ref][0][0], range_data[mapbox_ref][1][0]]
                lat_bounds = [range_data[mapbox_ref][0][1], range_data[mapbox_ref][1][1]]

                easting, northing = Tiles.lon_lat_to_easting_northing(lon_bounds, lat_bounds)
                new_bounds = (easting[0], northing[0], easting[1], northing[1])

                if cls.boundsx and cls.boundsy:
                    stream_data = dict(bounds=new_bounds)
                elif cls.boundsx:
                    stream_data = dict(boundsx=(new_bounds[0], new_bounds[2]))
                elif cls.boundsy:
                    stream_data = dict(boundsy=(new_bounds[1], new_bounds[3]))
                else:
                    stream_data = {}

                event_data[trace_uid] = stream_data


class BoundsXYCallback(BoundsCallback):
    boundsx = True
    boundsy = True


class BoundsXCallback(BoundsCallback):
    boundsx = True


class BoundsYCallback(BoundsCallback):
    boundsy = True


class RangeCallback(PlotlyCallback):
    callback_properties = ["viewport", "relayout_data"]
    x_range = False
    y_range = False

    @classmethod
    def get_event_data_from_property_update(cls, property, property_value, fig_dict):
        traces = fig_dict.get('data', [])

        if property == "viewport":
            event_data = cls.build_event_data_from_viewport(traces, property_value)
        else:
            event_data = cls.build_event_data_from_relayout_data(traces, property_value)

        return event_data

    @classmethod
    def build_event_data_from_viewport(cls, traces, property_value):
        # Process traces
        event_data = {}
        for trace in traces:
            trace_type = trace.get('type', 'scatter')
            trace_uid = trace.get('uid', None)

            if _trace_to_subplot.get(trace_type, None) != ['xaxis', 'yaxis']:
                continue

            xaxis = trace.get('xaxis', 'x').replace('x', 'xaxis')
            yaxis = trace.get('yaxis', 'y').replace('y', 'yaxis')
            xprop = f'{xaxis}.range'
            yprop = f'{yaxis}.range'

            if not property_value:
                x_range = None
                y_range = None
            elif xprop in property_value and yprop in property_value:
                x_range = tuple(property_value[xprop])
                y_range = tuple(property_value[yprop])
            elif xprop + "[0]" in property_value and xprop + "[1]" in property_value and \
                    yprop + "[0]" in property_value and yprop + "[1]" in property_value:
                x_range = (property_value[xprop + "[0]"],property_value[xprop + "[1]"])
                y_range = (property_value[yprop + "[0]"], property_value[yprop + "[1]"])
            else:
                continue

            stream_data = {}
            if cls.x_range:
                stream_data['x_range'] = x_range

            if cls.y_range:
                stream_data['y_range'] = y_range

            event_data[trace_uid] = stream_data
        return event_data

    @classmethod
    def build_event_data_from_relayout_data(cls, traces, property_value):
        # Process traces
        event_data = {}
        for trace in traces:
            trace_type = trace.get('type', PLOTLY_SCATTERMAP)
            trace_uid = trace.get('uid', None)

            if _trace_to_subplot.get(trace_type, None) != [PLOTLY_MAP]:
                continue

            subplot_id = trace.get("subplot", PLOTLY_MAP)
            derived_prop = subplot_id + "._derived"

            if not property_value:
                x_range = None
                y_range = None
            elif "coordinates" in property_value.get(derived_prop, {}):
                coords = property_value[derived_prop]["coordinates"]
                ((lon_top_left, lat_top_left),
                 (lon_top_right, lat_top_right),
                 (lon_bottom_right, lat_bottom_right),
                 (lon_bottom_left, lat_bottom_left)) = coords

                lon_left = min(lon_top_left, lon_bottom_left)
                lon_right = max(lon_top_right, lon_bottom_right)
                lat_bottom = min(lat_bottom_left, lat_bottom_right)
                lat_top = max(lat_top_left, lat_top_right)

                x_range, y_range = Tiles.lon_lat_to_easting_northing(
                    [lon_left, lon_right], [lat_bottom, lat_top]
                )
                x_range = tuple(x_range)
                y_range = tuple(y_range)
            else:
                continue

            stream_data = {}
            if cls.x_range:
                stream_data['x_range'] = x_range

            if cls.y_range:
                stream_data['y_range'] = y_range

            event_data[trace_uid] = stream_data

        return event_data


class RangeXYCallback(RangeCallback):
    x_range = True
    y_range = True


class RangeXCallback(RangeCallback):
    x_range = True


class RangeYCallback(RangeCallback):
    y_range = True


callbacks = Stream._callbacks['plotly']
callbacks[Selection1D] = Selection1DCallback
callbacks[SelectionXY] = BoundsXYCallback
callbacks[BoundsXY] = BoundsXYCallback
callbacks[BoundsX] = BoundsXCallback
callbacks[BoundsY] = BoundsYCallback
callbacks[RangeXY] = RangeXYCallback
callbacks[RangeX] = RangeXCallback
callbacks[RangeY] = RangeYCallback

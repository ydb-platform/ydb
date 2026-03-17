# -*- coding: utf-8 -*-
"""DataFrame client for InfluxDB v0.8."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import math
import warnings

from .client import InfluxDBClient


class DataFrameClient(InfluxDBClient):
    """Primary defintion of the DataFrameClient for v0.8.

    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    def __init__(self, ignore_nan=True, *args, **kwargs):
        """Initialize an instance of the DataFrameClient."""
        super(DataFrameClient, self).__init__(*args, **kwargs)

        try:
            global pd
            import pandas as pd
        except ImportError as ex:
            raise ImportError('DataFrameClient requires Pandas, '
                              '"{ex}" problem importing'.format(ex=str(ex)))

        self.EPOCH = pd.Timestamp('1970-01-01 00:00:00.000+00:00')
        self.ignore_nan = ignore_nan

    def write_points(self, data, *args, **kwargs):
        """Write to multiple time series names.

        :param data: A dictionary mapping series names to pandas DataFrames
        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        """
        batch_size = kwargs.get('batch_size')
        time_precision = kwargs.get('time_precision', 's')
        if batch_size:
            kwargs.pop('batch_size')  # don't hand over to InfluxDBClient
            for key, data_frame in data.items():
                number_batches = int(math.ceil(
                    len(data_frame) / float(batch_size)))
                for batch in range(number_batches):
                    start_index = batch * batch_size
                    end_index = (batch + 1) * batch_size
                    outdata = [
                        self._convert_dataframe_to_json(
                            name=key,
                            dataframe=data_frame
                            .iloc[start_index:end_index].copy(),
                            time_precision=time_precision)]
                    InfluxDBClient.write_points(self, outdata, *args, **kwargs)
            return True

        outdata = [
            self._convert_dataframe_to_json(name=key, dataframe=dataframe,
                                            time_precision=time_precision)
            for key, dataframe in data.items()]
        return InfluxDBClient.write_points(self, outdata, *args, **kwargs)

    def write_points_with_precision(self, data, time_precision='s'):
        """Write to multiple time series names.

        DEPRECATED
        """
        warnings.warn(
            "write_points_with_precision is deprecated, and will be removed "
            "in future versions. Please use "
            "``DataFrameClient.write_points(time_precision='..')`` instead.",
            FutureWarning)
        return self.write_points(data, time_precision='s')

    def query(self, query, time_precision='s', chunked=False):
        """Query data into DataFrames.

        Returns a DataFrame for a single time series and a map for multiple
        time series with the time series as value and its name as key.

        :param time_precision: [Optional, default 's'] Either 's', 'm', 'ms'
            or 'u'.
        :param chunked: [Optional, default=False] True if the data shall be
            retrieved in chunks, False otherwise.
        """
        result = InfluxDBClient.query(self, query=query,
                                      time_precision=time_precision,
                                      chunked=chunked)
        if len(result) == 0:
            return result
        elif len(result) == 1:
            return self._to_dataframe(result[0], time_precision)
        else:
            ret = {}
            for time_series in result:
                ret[time_series['name']] = self._to_dataframe(time_series,
                                                              time_precision)
            return ret

    @staticmethod
    def _to_dataframe(json_result, time_precision):
        dataframe = pd.DataFrame(data=json_result['points'],
                                 columns=json_result['columns'])
        if 'sequence_number' in dataframe.keys():
            dataframe.sort_values(['time', 'sequence_number'], inplace=True)
        else:
            dataframe.sort_values(['time'], inplace=True)

        pandas_time_unit = time_precision
        if time_precision == 'm':
            pandas_time_unit = 'ms'
        elif time_precision == 'u':
            pandas_time_unit = 'us'

        dataframe.index = pd.to_datetime(list(dataframe['time']),
                                         unit=pandas_time_unit,
                                         utc=True)
        del dataframe['time']
        return dataframe

    def _convert_dataframe_to_json(self, dataframe, name, time_precision='s'):
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.PeriodIndex) or
                isinstance(dataframe.index, pd.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or \
                            PeriodIndex.')

        if isinstance(dataframe.index, pd.PeriodIndex):
            dataframe.index = dataframe.index.to_timestamp()
        else:
            dataframe.index = pd.to_datetime(dataframe.index)

        if dataframe.index.tzinfo is None:
            dataframe.index = dataframe.index.tz_localize('UTC')
        dataframe['time'] = [self._datetime_to_epoch(dt, time_precision)
                             for dt in dataframe.index]
        data = {'name': name,
                'columns': [str(column) for column in dataframe.columns],
                'points': [self._convert_array(x) for x in dataframe.values]}
        return data

    def _convert_array(self, array):
        try:
            global np
            import numpy as np
        except ImportError as ex:
            raise ImportError('DataFrameClient requires Numpy, '
                              '"{ex}" problem importing'.format(ex=str(ex)))

        if self.ignore_nan:
            number_types = (int, float, np.number)
            condition = (all(isinstance(el, number_types) for el in array) and
                         np.isnan(array))
            return list(np.where(condition, None, array))

        return list(array)

    def _datetime_to_epoch(self, datetime, time_precision='s'):
        seconds = (datetime - self.EPOCH).total_seconds()
        if time_precision == 's':
            return seconds
        elif time_precision == 'm' or time_precision == 'ms':
            return seconds * 1000
        elif time_precision == 'u':
            return seconds * 1000000

# -*- coding: utf-8 -*-
"""DataFrame client for InfluxDB."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import math
from collections import defaultdict

import pandas as pd
import numpy as np

from .client import InfluxDBClient
from .line_protocol import _escape_tag


def _pandas_time_unit(time_precision):
    unit = time_precision
    if time_precision == 'm':
        unit = 'ms'
    elif time_precision == 'u':
        unit = 'us'
    elif time_precision == 'n':
        unit = 'ns'
    assert unit in ('s', 'ms', 'us', 'ns')
    return unit


def _escape_pandas_series(s):
    return s.apply(lambda v: _escape_tag(v))


class DataFrameClient(InfluxDBClient):
    """DataFrameClient instantiates InfluxDBClient to connect to the backend.

    The ``DataFrameClient`` object holds information necessary to connect
    to InfluxDB. Requests can be made to InfluxDB directly through the client.
    The client reads and writes from pandas DataFrames.
    """

    EPOCH = pd.Timestamp('1970-01-01 00:00:00.000+00:00')

    def write_points(self,
                     dataframe,
                     measurement,
                     tags=None,
                     tag_columns=None,
                     field_columns=None,
                     time_precision=None,
                     database=None,
                     retention_policy=None,
                     batch_size=None,
                     protocol='line',
                     numeric_precision=None):
        """Write to multiple time series names.

        :param dataframe: data points in a DataFrame
        :param measurement: name of measurement
        :param tags: dictionary of tags, with string key-values
        :param tag_columns: [Optional, default None] List of data tag names
        :param field_columns: [Options, default None] List of data field names
        :param time_precision: [Optional, default None] Either 's', 'ms', 'u'
            or 'n'.
        :param batch_size: [Optional] Value to write the points in batches
            instead of all at one time. Useful for when doing data dumps from
            one database to another or when doing a massive write operation
        :type batch_size: int
        :param protocol: Protocol for writing data. Either 'line' or 'json'.
        :param numeric_precision: Precision for floating point values.
            Either None, 'full' or some int, where int is the desired decimal
            precision. 'full' preserves full precision for int and float
            datatypes. Defaults to None, which preserves 14-15 significant
            figures for float and all significant figures for int datatypes.
        """
        if tag_columns is None:
            tag_columns = []

        if field_columns is None:
            field_columns = []

        if batch_size:
            number_batches = int(math.ceil(len(dataframe) / float(batch_size)))

            for batch in range(number_batches):
                start_index = batch * batch_size
                end_index = (batch + 1) * batch_size

                if protocol == 'line':
                    points = self._convert_dataframe_to_lines(
                        dataframe.iloc[start_index:end_index].copy(),
                        measurement=measurement,
                        global_tags=tags,
                        time_precision=time_precision,
                        tag_columns=tag_columns,
                        field_columns=field_columns,
                        numeric_precision=numeric_precision)
                else:
                    points = self._convert_dataframe_to_json(
                        dataframe.iloc[start_index:end_index].copy(),
                        measurement=measurement,
                        tags=tags,
                        time_precision=time_precision,
                        tag_columns=tag_columns,
                        field_columns=field_columns)

                super(DataFrameClient, self).write_points(
                    points,
                    time_precision,
                    database,
                    retention_policy,
                    protocol=protocol)

            return True

        if protocol == 'line':
            points = self._convert_dataframe_to_lines(
                dataframe,
                measurement=measurement,
                global_tags=tags,
                tag_columns=tag_columns,
                field_columns=field_columns,
                time_precision=time_precision,
                numeric_precision=numeric_precision)
        else:
            points = self._convert_dataframe_to_json(
                dataframe,
                measurement=measurement,
                tags=tags,
                time_precision=time_precision,
                tag_columns=tag_columns,
                field_columns=field_columns)

        super(DataFrameClient, self).write_points(
            points,
            time_precision,
            database,
            retention_policy,
            protocol=protocol)

        return True

    def query(self,
              query,
              params=None,
              bind_params=None,
              epoch=None,
              expected_response_code=200,
              database=None,
              raise_errors=True,
              chunked=False,
              chunk_size=0,
              method="GET",
              dropna=True,
              data_frame_index=None):
        """
        Query data into a DataFrame.

        .. danger::
            In order to avoid injection vulnerabilities (similar to `SQL
            injection <https://www.owasp.org/index.php/SQL_Injection>`_
            vulnerabilities), do not directly include untrusted data into the
            ``query`` parameter, use ``bind_params`` instead.

        :param query: the actual query string
        :param params: additional parameters for the request, defaults to {}
        :param bind_params: bind parameters for the query:
            any variable in the query written as ``'$var_name'`` will be
            replaced with ``bind_params['var_name']``. Only works in the
            ``WHERE`` clause and takes precedence over ``params['params']``
        :param epoch: response timestamps to be in epoch format either 'h',
            'm', 's', 'ms', 'u', or 'ns',defaults to `None` which is
            RFC3339 UTC format with nanosecond precision
        :param expected_response_code: the expected status code of response,
            defaults to 200
        :param database: database to query, defaults to None
        :param raise_errors: Whether or not to raise exceptions when InfluxDB
            returns errors, defaults to True
        :param chunked: Enable to use chunked responses from InfluxDB.
            With ``chunked`` enabled, one ResultSet is returned per chunk
            containing all results within that chunk
        :param chunk_size: Size of each chunk to tell InfluxDB to use.
        :param dropna: drop columns where all values are missing
        :param data_frame_index: the list of columns that
            are used as DataFrame index
        :returns: the queried data
        :rtype: :class:`~.ResultSet`
        """
        query_args = dict(params=params,
                          bind_params=bind_params,
                          epoch=epoch,
                          expected_response_code=expected_response_code,
                          raise_errors=raise_errors,
                          chunked=chunked,
                          database=database,
                          method=method,
                          chunk_size=chunk_size)
        results = super(DataFrameClient, self).query(query, **query_args)
        if query.strip().upper().startswith("SELECT"):
            if len(results) > 0:
                return self._to_dataframe(results, dropna,
                                          data_frame_index=data_frame_index)
            else:
                return {}
        else:
            return results

    def _to_dataframe(self, rs, dropna=True, data_frame_index=None):
        result = defaultdict(list)
        if isinstance(rs, list):
            return map(self._to_dataframe, rs,
                       [dropna for _ in range(len(rs))])

        for key, data in rs.items():
            name, tags = key
            if tags is None:
                key = name
            else:
                key = (name, tuple(sorted(tags.items())))
            df = pd.DataFrame(data)
            df.time = pd.to_datetime(df.time)

            if data_frame_index:
                df.set_index(data_frame_index, inplace=True)
            else:
                df.set_index('time', inplace=True)
                if df.index.tzinfo is None:
                    df.index = df.index.tz_localize('UTC')
                df.index.name = None

            result[key].append(df)
        for key, data in result.items():
            df = pd.concat(data).sort_index()
            if dropna:
                df.dropna(how='all', axis=1, inplace=True)
            result[key] = df

        return result

    @staticmethod
    def _convert_dataframe_to_json(dataframe,
                                   measurement,
                                   tags=None,
                                   tag_columns=None,
                                   field_columns=None,
                                   time_precision=None):

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.PeriodIndex) or
                isinstance(dataframe.index, pd.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or '
                            'PeriodIndex.')

        # Make sure tags and tag columns are correctly typed
        tag_columns = tag_columns if tag_columns is not None else []
        field_columns = field_columns if field_columns is not None else []
        tags = tags if tags is not None else {}
        # Assume field columns are all columns not included in tag columns
        if not field_columns:
            field_columns = list(
                set(dataframe.columns).difference(set(tag_columns)))

        if not isinstance(dataframe.index, pd.DatetimeIndex):
            dataframe.index = pd.to_datetime(dataframe.index)
        if dataframe.index.tzinfo is None:
            dataframe.index = dataframe.index.tz_localize('UTC')

        # Convert column to strings
        dataframe.columns = dataframe.columns.astype('str')

        # Convert dtype for json serialization
        dataframe = dataframe.astype('object')

        precision_factor = {
            "n": 1,
            "u": 1e3,
            "ms": 1e6,
            "s": 1e9,
            "m": 1e9 * 60,
            "h": 1e9 * 3600,
        }.get(time_precision, 1)

        if not tag_columns:
            points = [
                {'measurement': measurement,
                 'fields':
                    rec.replace([np.inf, -np.inf], np.nan).dropna().to_dict(),
                 'time': np.int64(ts.value / precision_factor)}
                for ts, (_, rec) in zip(
                    dataframe.index,
                    dataframe[field_columns].iterrows()
                )
            ]

            return points

        points = [
            {'measurement': measurement,
             'tags': dict(list(tag.items()) + list(tags.items())),
             'fields':
                rec.replace([np.inf, -np.inf], np.nan).dropna().to_dict(),
             'time': np.int64(ts.value / precision_factor)}
            for ts, tag, (_, rec) in zip(
                dataframe.index,
                dataframe[tag_columns].to_dict('record'),
                dataframe[field_columns].iterrows()
            )
        ]

        return points

    def _convert_dataframe_to_lines(self,
                                    dataframe,
                                    measurement,
                                    field_columns=None,
                                    tag_columns=None,
                                    global_tags=None,
                                    time_precision=None,
                                    numeric_precision=None):

        dataframe = dataframe.dropna(how='all').copy()
        if len(dataframe) == 0:
            return []

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('Must be DataFrame, but type was: {0}.'
                            .format(type(dataframe)))
        if not (isinstance(dataframe.index, pd.PeriodIndex) or
                isinstance(dataframe.index, pd.DatetimeIndex)):
            raise TypeError('Must be DataFrame with DatetimeIndex or '
                            'PeriodIndex.')

        dataframe = dataframe.rename(
            columns={item: _escape_tag(item) for item in dataframe.columns})
        # Create a Series of columns for easier indexing
        column_series = pd.Series(dataframe.columns)

        if field_columns is None:
            field_columns = []

        if tag_columns is None:
            tag_columns = []

        if global_tags is None:
            global_tags = {}

        # Make sure field_columns and tag_columns are lists
        field_columns = list(field_columns) if list(field_columns) else []
        tag_columns = list(tag_columns) if list(tag_columns) else []

        # If field columns but no tag columns, assume rest of columns are tags
        if field_columns and (not tag_columns):
            tag_columns = list(column_series[~column_series.isin(
                field_columns)])

        # If no field columns, assume non-tag columns are fields
        if not field_columns:
            field_columns = list(column_series[~column_series.isin(
                tag_columns)])

        precision_factor = {
            "n": 1,
            "u": 1e3,
            "ms": 1e6,
            "s": 1e9,
            "m": 1e9 * 60,
            "h": 1e9 * 3600,
        }.get(time_precision, 1)

        # Make array of timestamp ints
        if isinstance(dataframe.index, pd.PeriodIndex):
            time = ((dataframe.index.to_timestamp().values.astype(np.int64) //
                     precision_factor).astype(np.int64).astype(str))
        else:
            time = ((pd.to_datetime(dataframe.index).values.astype(np.int64) //
                     precision_factor).astype(np.int64).astype(str))

        # If tag columns exist, make an array of formatted tag keys and values
        if tag_columns:

            # Make global_tags as tag_columns
            if global_tags:
                for tag in global_tags:
                    dataframe[tag] = global_tags[tag]
                    tag_columns.append(tag)

            tag_df = dataframe[tag_columns]
            tag_df = tag_df.fillna('')  # replace NA with empty string
            tag_df = tag_df.sort_index(axis=1)
            tag_df = self._stringify_dataframe(
                tag_df, numeric_precision, datatype='tag')

            # join prepended tags, leaving None values out
            tags = tag_df.apply(
                lambda s: [',' + s.name + '=' + v if v else '' for v in s])
            tags = tags.sum(axis=1)

            del tag_df
        elif global_tags:
            tag_string = ''.join(
                [",{}={}".format(k, _escape_tag(v))
                 if v not in [None, ''] else ""
                 for k, v in sorted(global_tags.items())]
            )
            tags = pd.Series(tag_string, index=dataframe.index)
        else:
            tags = ''

        # Make an array of formatted field keys and values
        field_df = dataframe[field_columns].replace([np.inf, -np.inf], np.nan)
        nans = pd.isnull(field_df)

        field_df = self._stringify_dataframe(field_df,
                                             numeric_precision,
                                             datatype='field')

        field_df = (field_df.columns.values + '=').tolist() + field_df
        field_df[field_df.columns[1:]] = ',' + field_df[field_df.columns[1:]]
        field_df[nans] = ''

        fields = field_df.sum(axis=1).map(lambda x: x.lstrip(','))
        del field_df

        # Generate line protocol string
        measurement = _escape_tag(measurement)
        points = (measurement + tags + ' ' + fields + ' ' + time).tolist()
        return points

    @staticmethod
    def _stringify_dataframe(dframe, numeric_precision, datatype='field'):

        # Prevent modification of input dataframe
        dframe = dframe.copy()

        # Find int and string columns for field-type data
        int_columns = dframe.select_dtypes(include=['integer']).columns
        string_columns = dframe.select_dtypes(include=['object']).columns

        # Convert dframe to string
        if numeric_precision is None:
            # If no precision specified, convert directly to string (fast)
            dframe = dframe.astype(str)
        elif numeric_precision == 'full':
            # If full precision, use repr to get full float precision
            float_columns = (dframe.select_dtypes(
                include=['floating']).columns)
            nonfloat_columns = dframe.columns[~dframe.columns.isin(
                float_columns)]
            dframe[float_columns] = dframe[float_columns].applymap(repr)
            dframe[nonfloat_columns] = (dframe[nonfloat_columns].astype(str))
        elif isinstance(numeric_precision, int):
            # If precision is specified, round to appropriate precision
            float_columns = (dframe.select_dtypes(
                include=['floating']).columns)
            nonfloat_columns = dframe.columns[~dframe.columns.isin(
                float_columns)]
            dframe[float_columns] = (dframe[float_columns].round(
                numeric_precision))

            # If desired precision is > 10 decimal places, need to use repr
            if numeric_precision > 10:
                dframe[float_columns] = (dframe[float_columns].applymap(repr))
                dframe[nonfloat_columns] = (dframe[nonfloat_columns]
                                            .astype(str))
            else:
                dframe = dframe.astype(str)
        else:
            raise ValueError('Invalid numeric precision.')

        if datatype == 'field':
            # If dealing with fields, format ints and strings correctly
            dframe[int_columns] += 'i'
            dframe[string_columns] = '"' + dframe[string_columns] + '"'
        elif datatype == 'tag':
            dframe = dframe.apply(_escape_pandas_series)

        dframe.columns = dframe.columns.astype(str)

        return dframe

    def _datetime_to_epoch(self, datetime, time_precision='s'):
        seconds = (datetime - self.EPOCH).total_seconds()
        if time_precision == 'h':
            return seconds / 3600
        elif time_precision == 'm':
            return seconds / 60
        elif time_precision == 's':
            return seconds
        elif time_precision == 'ms':
            return seconds * 1e3
        elif time_precision == 'u':
            return seconds * 1e6
        elif time_precision == 'n':
            return seconds * 1e9

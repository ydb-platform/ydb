# -*- coding: utf-8 -*-
"""Helper class for InfluxDB."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple, defaultdict
from datetime import datetime
from warnings import warn

import six


class SeriesHelper(object):
    """Subclass this helper eases writing data points in bulk.

    All data points are immutable, ensuring they do not get overwritten.
    Each subclass can write to its own database.
    The time series names can also be based on one or more defined fields.
    The field "time" can be specified when creating a point, and may be any of
    the time types supported by the client (i.e. str, datetime, int).
    If the time is not specified, the current system time (utc) will be used.

    Annotated example::

        class MySeriesHelper(SeriesHelper):
            class Meta:
                # Meta class stores time series helper configuration.
                series_name = 'events.stats.{server_name}'
                # Series name must be a string, curly brackets for dynamic use.
                fields = ['time', 'server_name']
                # Defines all the fields in this time series.
                ### Following attributes are optional. ###
                client = TestSeriesHelper.client
                # Client should be an instance of InfluxDBClient.
                :warning: Only used if autocommit is True.
                bulk_size = 5
                # Defines the number of data points to write simultaneously.
                # Only applicable if autocommit is True.
                autocommit = True
                # If True and no bulk_size, then will set bulk_size to 1.
                retention_policy = 'your_retention_policy'
                # Specify the retention policy for the data points
                time_precision = "h"|"m"|s"|"ms"|"u"|"ns"
                # Default is ns (nanoseconds)
                # Setting time precision while writing point
                # You should also make sure time is set in the given precision

    """

    __initialized__ = False

    def __new__(cls, *args, **kwargs):
        """Initialize class attributes for subsequent constructor calls.

        :note: *args and **kwargs are not explicitly used in this function,
        but needed for Python 2 compatibility.
        """
        if not cls.__initialized__:
            cls.__initialized__ = True
            try:
                _meta = getattr(cls, 'Meta')
            except AttributeError:
                raise AttributeError(
                    'Missing Meta class in {0}.'.format(
                        cls.__name__))

            for attr in ['series_name', 'fields', 'tags']:
                try:
                    setattr(cls, '_' + attr, getattr(_meta, attr))
                except AttributeError:
                    raise AttributeError(
                        'Missing {0} in {1} Meta class.'.format(
                            attr,
                            cls.__name__))

            cls._autocommit = getattr(_meta, 'autocommit', False)
            cls._time_precision = getattr(_meta, 'time_precision', None)

            allowed_time_precisions = ['h', 'm', 's', 'ms', 'u', 'ns', None]
            if cls._time_precision not in allowed_time_precisions:
                raise AttributeError(
                    'In {}, time_precision is set, but invalid use any of {}.'
                    .format(cls.__name__, ','.join(allowed_time_precisions)))

            cls._retention_policy = getattr(_meta, 'retention_policy', None)

            cls._client = getattr(_meta, 'client', None)
            if cls._autocommit and not cls._client:
                raise AttributeError(
                    'In {0}, autocommit is set to True, but no client is set.'
                    .format(cls.__name__))

            try:
                cls._bulk_size = getattr(_meta, 'bulk_size')
                if cls._bulk_size < 1 and cls._autocommit:
                    warn(
                        'Definition of bulk_size in {0} forced to 1, '
                        'was less than 1.'.format(cls.__name__))
                    cls._bulk_size = 1
            except AttributeError:
                cls._bulk_size = -1
            else:
                if not cls._autocommit:
                    warn(
                        'Definition of bulk_size in {0} has no affect because'
                        ' autocommit is false.'.format(cls.__name__))

            cls._datapoints = defaultdict(list)

            if 'time' in cls._fields:
                cls._fields.remove('time')
            cls._type = namedtuple(cls.__name__,
                                   ['time'] + cls._tags + cls._fields)
            cls._type.__new__.__defaults__ = (None,) * len(cls._fields)

        return super(SeriesHelper, cls).__new__(cls)

    def __init__(self, **kw):
        """Call to constructor creates a new data point.

        :note: Data points written when `bulk_size` is reached per Helper.
        :warning: Data points are *immutable* (`namedtuples`).
        """
        cls = self.__class__
        timestamp = kw.pop('time', self._current_timestamp())
        tags = set(cls._tags)
        fields = set(cls._fields)
        keys = set(kw.keys())

        # all tags should be passed, and keys - tags should be a subset of keys
        if not (tags <= keys):
            raise NameError(
                'Expected arguments to contain all tags {0}, instead got {1}.'
                .format(cls._tags, kw.keys()))
        if not (keys - tags <= fields):
            raise NameError('Got arguments not in tags or fields: {0}'
                            .format(keys - tags - fields))

        cls._datapoints[cls._series_name.format(**kw)].append(
            cls._type(time=timestamp, **kw)
        )

        if cls._autocommit and \
                sum(len(series) for series in cls._datapoints.values()) \
                >= cls._bulk_size:
            cls.commit()

    @classmethod
    def commit(cls, client=None):
        """Commit everything from datapoints via the client.

        :param client: InfluxDBClient instance for writing points to InfluxDB.
        :attention: any provided client will supersede the class client.
        :return: result of client.write_points.
        """
        if not client:
            client = cls._client

        rtn = client.write_points(
            cls._json_body_(),
            time_precision=cls._time_precision,
            retention_policy=cls._retention_policy)
        # will be None if not set and will default to ns
        cls._reset_()
        return rtn

    @classmethod
    def _json_body_(cls):
        """Return the JSON body of given datapoints.

        :return: JSON body of these datapoints.
        """
        json = []
        if not cls.__initialized__:
            cls._reset_()
        for series_name, data in six.iteritems(cls._datapoints):
            for point in data:
                json_point = {
                    "measurement": series_name,
                    "fields": {},
                    "tags": {},
                    "time": getattr(point, "time")
                }

                for field in cls._fields:
                    value = getattr(point, field)
                    if value is not None:
                        json_point['fields'][field] = value

                for tag in cls._tags:
                    json_point['tags'][tag] = getattr(point, tag)

                json.append(json_point)
        return json

    @classmethod
    def _reset_(cls):
        """Reset data storage."""
        cls._datapoints = defaultdict(list)

    @staticmethod
    def _current_timestamp():
        return datetime.utcnow()

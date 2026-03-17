# -*- coding: utf-8 -*-
"""Helper class for InfluxDB for v0.8."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import namedtuple, defaultdict
from warnings import warn

import six


class SeriesHelper(object):
    """Define the SeriesHelper object for InfluxDB v0.8.

    Subclassing this helper eases writing data points in bulk.
    All data points are immutable, ensuring they do not get overwritten.
    Each subclass can write to its own database.
    The time series names can also be based on one or more defined fields.

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

            for attr in ['series_name', 'fields']:
                try:
                    setattr(cls, '_' + attr, getattr(_meta, attr))
                except AttributeError:
                    raise AttributeError(
                        'Missing {0} in {1} Meta class.'.format(
                            attr,
                            cls.__name__))

            cls._autocommit = getattr(_meta, 'autocommit', False)

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
            cls._type = namedtuple(cls.__name__, cls._fields)

        return super(SeriesHelper, cls).__new__(cls)

    def __init__(self, **kw):
        """Create a new data point.

        All fields must be present.

        :note: Data points written when `bulk_size` is reached per Helper.
        :warning: Data points are *immutable* (`namedtuples`).
        """
        cls = self.__class__

        if sorted(cls._fields) != sorted(kw.keys()):
            raise NameError(
                'Expected {0}, got {1}.'.format(
                    cls._fields,
                    kw.keys()))

        cls._datapoints[cls._series_name.format(**kw)].append(cls._type(**kw))

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
        rtn = client.write_points(cls._json_body_())
        cls._reset_()
        return rtn

    @classmethod
    def _json_body_(cls):
        """Return JSON body of the datapoints.

        :return: JSON body of the datapoints.
        """
        json = []
        if not cls.__initialized__:
            cls._reset_()
        for series_name, data in six.iteritems(cls._datapoints):
            json.append({'name': series_name,
                         'columns': cls._fields,
                         'points': [[getattr(point, k) for k in cls._fields]
                                    for point in data]
                         })
        return json

    @classmethod
    def _reset_(cls):
        """Reset data storage."""
        cls._datapoints = defaultdict(list)

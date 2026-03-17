# -*- coding: utf-8 -*-
"""Module to prepare the resultset."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import warnings

from influxdb.exceptions import InfluxDBClientError

_sentinel = object()


class ResultSet(object):
    """A wrapper around a single InfluxDB query result."""

    def __init__(self, series, raise_errors=True):
        """Initialize the ResultSet."""
        self._raw = series
        self._error = self._raw.get('error', None)

        if self.error is not None and raise_errors is True:
            raise InfluxDBClientError(self.error)

    @property
    def raw(self):
        """Raw JSON from InfluxDB."""
        return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    @property
    def error(self):
        """Error returned by InfluxDB."""
        return self._error

    def __getitem__(self, key):
        """Retrieve the series name or specific set based on key.

        :param key: Either a series name, or a tags_dict, or
                    a 2-tuple(series_name, tags_dict).
                    If the series name is None (or not given) then any serie
                    matching the eventual given tags will be given its points
                    one after the other.
                    To get the points of every series in this resultset then
                    you have to provide None as key.
        :return: A generator yielding `Point`s matching the given key.
        NB:
        The order in which the points are yielded is actually undefined but
        it might change..
        """
        warnings.warn(
            ("ResultSet's ``__getitem__`` method will be deprecated. Use"
             "``get_points`` instead."),
            DeprecationWarning
        )

        if isinstance(key, tuple):
            if len(key) != 2:
                raise TypeError('only 2-tuples allowed')

            name = key[0]
            tags = key[1]

            if not isinstance(tags, dict) and tags is not None:
                raise TypeError('tags should be a dict')
        elif isinstance(key, dict):
            name = None
            tags = key
        else:
            name = key
            tags = None

        return self.get_points(name, tags)

    def get_points(self, measurement=None, tags=None):
        """Return a generator for all the points that match the given filters.

        :param measurement: The measurement name
        :type measurement: str

        :param tags: Tags to look for
        :type tags: dict

        :return: Points generator
        """
        # Raise error if measurement is not str or bytes
        if not isinstance(measurement,
                          (bytes, type(b''.decode()), type(None))):
            raise TypeError('measurement must be an str or None')

        for series in self._get_series():
            series_name = series.get('measurement',
                                     series.get('name', 'results'))
            if series_name is None:
                # this is a "system" query or a query which
                # doesn't return a name attribute.
                # like 'show retention policies' ..
                if tags is None:
                    for item in self._get_points_for_series(series):
                        yield item

            elif measurement in (None, series_name):
                # by default if no tags was provided then
                # we will matches every returned series
                series_tags = series.get('tags', {})
                for item in self._get_points_for_series(series):
                    if tags is None or \
                            self._tag_matches(item, tags) or \
                            self._tag_matches(series_tags, tags):
                        yield item

    def __repr__(self):
        """Representation of ResultSet object."""
        items = []

        for item in self.items():
            items.append("'%s': %s" % (item[0], list(item[1])))

        return "ResultSet({%s})" % ", ".join(items)

    def __iter__(self):
        """Yield one dict instance per series result."""
        for key in self.keys():
            yield list(self.__getitem__(key))

    @staticmethod
    def _tag_matches(tags, filter):
        """Check if all key/values in filter match in tags."""
        for tag_name, tag_value in filter.items():
            # using _sentinel as I'm not sure that "None"
            # could be used, because it could be a valid
            # series_tags value : when a series has no such tag
            # then I think it's set to /null/None/.. TBC..
            series_tag_value = tags.get(tag_name, _sentinel)
            if series_tag_value != tag_value:
                return False

        return True

    def _get_series(self):
        """Return all series."""
        return self.raw.get('series', [])

    def __len__(self):
        """Return the len of the keys in the ResultSet."""
        return len(self.keys())

    def keys(self):
        """Return the list of keys in the ResultSet.

        :return: List of keys. Keys are tuples (series_name, tags)
        """
        keys = []
        for series in self._get_series():
            keys.append(
                (series.get('measurement',
                            series.get('name', 'results')),
                 series.get('tags', None))
            )
        return keys

    def items(self):
        """Return the set of items from the ResultSet.

        :return: List of tuples, (key, generator)
        """
        items = []
        for series in self._get_series():
            series_key = (series.get('measurement',
                                     series.get('name', 'results')),
                          series.get('tags', None))
            items.append(
                (series_key, self._get_points_for_series(series))
            )
        return items

    def _get_points_for_series(self, series):
        """Return generator of dict from columns and values of a series.

        :param series: One series
        :return: Generator of dicts
        """
        for point in series.get('values', []):
            yield self.point_from_cols_vals(
                series['columns'],
                point
            )

    @staticmethod
    def point_from_cols_vals(cols, vals):
        """Create a dict from columns and values lists.

        :param cols: List of columns
        :param vals: List of values
        :return: Dict where keys are columns.
        """
        point = {}
        for col_index, col_name in enumerate(cols):
            point[col_name] = vals[col_index]

        return point

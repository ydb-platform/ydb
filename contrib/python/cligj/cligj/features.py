"""Feature parsing and normalization"""

from itertools import chain
import json
import re

import click


def normalize_feature_inputs(ctx, param, value):
    """Click callback that normalizes feature input values.

    Returns a generator over features from the input value.

    Parameters
    ----------
    ctx: a Click context
    param: the name of the argument or option
    value: object
        The value argument may be one of the following:

        1. A list of paths to files containing GeoJSON feature
           collections or feature sequences.
        2. A list of string-encoded coordinate pairs of the form
           "[lng, lat]", or "lng, lat", or "lng lat".

        If no value is provided, features will be read from stdin.

    Yields
    ------
    Mapping
        A GeoJSON Feature represented by a Python mapping

    """
    for feature_like in value or ('-',):
        try:
            with click.open_file(feature_like, encoding="utf-8") as src:
                for feature in iter_features(iter(src)):
                    yield feature
        except IOError:
            coords = list(coords_from_query(feature_like))
            yield {
                'type': 'Feature',
                'properties': {},
                'geometry': {
                    'type': 'Point',
                    'coordinates': coords}}


def iter_features(geojsonfile, func=None):
    """Extract GeoJSON features from a text file object.

    Given a file-like object containing a single GeoJSON feature
    collection text or a sequence of GeoJSON features, iter_features()
    iterates over lines of the file and yields GeoJSON features.

    Parameters
    ----------
    geojsonfile: a file-like object
        The geojsonfile implements the iterator protocol and yields
        lines of JSON text.
    func: function, optional
        A function that will be applied to each extracted feature. It
        takes a feature object and may return a replacement feature or
        None -- in which case iter_features does not yield.

    Yields
    ------
    Mapping
        A GeoJSON Feature represented by a Python mapping

    """
    func = func or (lambda x: x)
    first_line = next(geojsonfile)

    # Does the geojsonfile contain RS-delimited JSON sequences?
    if first_line.startswith(u'\x1e'):
        text_buffer = first_line.strip(u'\x1e')
        for line in geojsonfile:
            if line.startswith(u'\x1e'):
                if text_buffer:
                    obj = json.loads(text_buffer)
                    if 'coordinates' in obj:
                        obj = to_feature(obj)
                    newfeat = func(obj)
                    if newfeat:
                        yield newfeat
                text_buffer = line.strip(u'\x1e')
            else:
                text_buffer += line
        # complete our parsing with a for-else clause.
        else:
            obj = json.loads(text_buffer)
            if 'coordinates' in obj:
                obj = to_feature(obj)
            newfeat = func(obj)
            if newfeat:
                yield newfeat

    # If not, it may contains LF-delimited GeoJSON objects or a single
    # multi-line pretty-printed GeoJSON object.
    else:
        # Try to parse LF-delimited sequences of features or feature
        # collections produced by, e.g., `jq -c ...`.
        try:
            obj = json.loads(first_line)
            if obj['type'] == 'Feature':
                newfeat = func(obj)
                if newfeat:
                    yield newfeat
                for line in geojsonfile:
                    newfeat = func(json.loads(line))
                    if newfeat:
                        yield newfeat
            elif obj['type'] == 'FeatureCollection':
                for feat in obj['features']:
                    newfeat = func(feat)
                    if newfeat:
                        yield newfeat
            elif 'coordinates' in obj:
                newfeat = func(to_feature(obj))
                if newfeat:
                    yield newfeat
                for line in geojsonfile:
                    newfeat = func(to_feature(json.loads(line)))
                    if newfeat:
                        yield newfeat

        # Indented or pretty-printed GeoJSON features or feature
        # collections will fail out of the try clause above since
        # they'll have no complete JSON object on their first line.
        # To handle these, we slurp in the entire file and parse its
        # text.
        except ValueError:
            text = "".join(chain([first_line], geojsonfile))
            obj = json.loads(text)
            if obj['type'] == 'Feature':
                newfeat = func(obj)
                if newfeat:
                    yield newfeat
            elif obj['type'] == 'FeatureCollection':
                for feat in obj['features']:
                    newfeat = func(feat)
                    if newfeat:
                        yield newfeat
            elif 'coordinates' in obj:
                newfeat = func(to_feature(obj))
                if newfeat:
                    yield newfeat


def to_feature(obj):
    """Converts an object to a GeoJSON Feature

    Returns feature verbatim or wraps geom in a feature with empty
    properties.

    Raises
    ------
    ValueError

    Returns
    -------
    Mapping
        A GeoJSON Feature represented by a Python mapping

    """
    if obj['type'] == 'Feature':
        return obj
    elif 'coordinates' in obj:
        return {
            'type': 'Feature',
            'properties': {},
            'geometry': obj}
    else:
        raise ValueError("Object is not a feature or geometry")


def iter_query(query):
    """Accept a filename, stream, or string.
    Returns an iterator over lines of the query."""
    try:
        itr = click.open_file(query).readlines()
    except IOError:
        itr = [query]
    return itr


def coords_from_query(query):
    """Transform a query line into a (lng, lat) pair of coordinates."""
    try:
        coords = json.loads(query)
    except ValueError:
        query = query.replace(',', ' ')
        vals = query.split()
        coords = [float(v) for v in vals]
    return tuple(coords[:2])


def normalize_feature_objects(feature_objs):
    """Takes an iterable of GeoJSON-like Feature mappings or
    an iterable of objects with a geo interface and
    normalizes it to the former."""
    for obj in feature_objs:
        if (
            hasattr(obj, "__geo_interface__")
            and "type" in obj.__geo_interface__.keys()
            and obj.__geo_interface__["type"] == "Feature"
        ):
            yield obj.__geo_interface__
        elif isinstance(obj, dict) and "type" in obj and obj["type"] == "Feature":
            yield obj
        else:
            raise ValueError("Did not recognize object as GeoJSON Feature")

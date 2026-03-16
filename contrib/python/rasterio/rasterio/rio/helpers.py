"""
Helper objects used by multiple CLI commands.
"""

import json
import os

import click

from rasterio.errors import FileOverwriteError


def coords(obj):
    """Yield all coordinate coordinate tuples from a geometry or feature.
    From python-geojson package."""
    if isinstance(obj, (tuple, list)):
        coordinates = obj
    elif 'geometry' in obj:
        coordinates = obj['geometry']['coordinates']
    else:
        coordinates = obj.get('coordinates', obj)
    for e in coordinates:
        if isinstance(e, (float, int)):
            yield tuple(coordinates)
            break
        else:
            yield from coords(e)


def write_features(
        fobj, collection, sequence=False, geojson_type='feature', use_rs=False,
        **dump_kwds):
    """Read an iterator of (feat, bbox) pairs and write to file using
    the selected modes."""
    # Sequence of features expressed as bbox, feature, or collection.
    if sequence:
        for feat in collection():
            xs, ys = zip(*coords(feat))
            bbox = (min(xs), min(ys), max(xs), max(ys))
            if use_rs:
                fobj.write("\u001e")
            if geojson_type == "bbox":
                fobj.write(json.dumps(bbox, **dump_kwds))
            else:
                fobj.write(json.dumps(feat, **dump_kwds))
            fobj.write('\n')

    # Aggregate all features into a single object expressed as
    # bbox or collection.
    else:
        features = list(collection())
        if geojson_type == 'bbox':
            fobj.write(json.dumps(collection.bbox, **dump_kwds))
        else:
            fobj.write(json.dumps({
                'bbox': collection.bbox,
                'type': 'FeatureCollection',
                'features': features},
                **dump_kwds))
        fobj.write('\n')


def resolve_inout(
    input=None, output=None, files=None, overwrite=False, num_inputs=None
):
    """Resolves inputs and outputs from standard args and options.

    Parameters
    ----------
    input : str
        A single input filename, optional.
    output : str
        A single output filename, optional.
    files : str
        A sequence of filenames in which the last is the output filename.
    overwrite : bool
        Whether to force overwriting the output file.
    num_inputs : int
        Raise exceptions if the number of resolved input files is higher
        or lower than this number.

    Returns
    -------
    tuple (str, list of str)
        The resolved output filename and input filenames as a tuple of
        length 2.

    If provided, the output file may be overwritten. An output
    file extracted from files will not be overwritten unless
    overwrite is True.

    Raises
    ------
    click.BadParameter

    """
    resolved_output = output or (files[-1] if files else None)

    if not overwrite and resolved_output and os.path.exists(resolved_output):
        raise FileOverwriteError(
            "file exists and won't be overwritten without use of the `--overwrite` option."
        )

    resolved_inputs = (
        [input] if input else [] +
        list(files[:-1 if not output else None]) if files else [])

    if num_inputs is not None:
        if len(resolved_inputs) < num_inputs:
            raise click.BadParameter("Insufficient inputs")
        elif len(resolved_inputs) > num_inputs:
            raise click.BadParameter("Too many inputs")

    return resolved_output, resolved_inputs


def to_lower(ctx, param, value):
    """Click callback, converts values to lowercase."""
    return value.lower()

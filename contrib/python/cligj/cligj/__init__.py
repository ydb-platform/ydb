"""cligj

A package of arguments, options, and parsers for the Python GeoJSON
ecosystem.
"""

import sys
from warnings import warn

import click

from .features import normalize_feature_inputs

__version__ = "0.7.2"

if sys.version_info < (3, 6):
    warn("cligj 1.0.0 will require Python >= 3.6", FutureWarning)


# Multiple input files.
files_in_arg = click.argument(
    'files',
    nargs=-1,
    type=click.Path(resolve_path=True),
    required=True,
    metavar="INPUTS...")


# Multiple files, last of which is an output file.
files_inout_arg = click.argument(
    'files',
    nargs=-1,
    type=click.Path(resolve_path=True),
    required=True,
    metavar="INPUTS... OUTPUT")


# Features from files, command line args, or stdin.
# Returns the input data as an iterable of GeoJSON Feature-like
# dictionaries.
features_in_arg = click.argument(
    'features',
    nargs=-1,
    callback=normalize_feature_inputs,
    metavar="FEATURES...")


# Options.
verbose_opt = click.option(
    '--verbose', '-v',
    count=True,
    help="Increase verbosity.")

quiet_opt = click.option(
    '--quiet', '-q',
    count=True,
    help="Decrease verbosity.")

# Format driver option.
format_opt = click.option(
    '-f', '--format', '--driver', 'driver',
    default='GTiff',
    help="Output format driver")

# JSON formatting options.
indent_opt = click.option(
    '--indent',
    type=int,
    default=None,
    help="Indentation level for JSON output")

compact_opt = click.option(
    '--compact/--not-compact',
    default=False,
    help="Use compact separators (',', ':').")

# Coordinate precision option.
precision_opt = click.option(
    '--precision',
    type=int,
    default=-1,
    help="Decimal precision of coordinates.")

# Geographic (default), projected, or Mercator switch.
projection_geographic_opt = click.option(
    '--geographic',
    'projection',
    flag_value='geographic',
    default=True,
    help="Output in geographic coordinates (the default).")

projection_projected_opt = click.option(
    '--projected',
    'projection',
    flag_value='projected',
    help="Output in dataset's own, projected coordinates.")

projection_mercator_opt = click.option(
    '--mercator',
    'projection',
    flag_value='mercator',
    help="Output in Web Mercator coordinates.")

# Feature collection or feature sequence switch.
sequence_opt = click.option(
    '--sequence/--no-sequence',
    default=False,
    help="Write a LF-delimited sequence of texts containing individual "
    "objects or write a single JSON text containing a feature "
    "collection object (the default).",
    callback=lambda ctx, param, value: warn(
        "Sequences of Features, not FeatureCollections, will be the default in version 1.0.0",
        FutureWarning,
    )
    or value,
)

use_rs_opt = click.option(
    '--rs/--no-rs',
    'use_rs',
    default=False,
    help="Use RS (0x1E) as a prefix for individual texts in a sequence "
         "as per http://tools.ietf.org/html/draft-ietf-json-text-sequence-13 "
         "(default is False).")


def geojson_type_collection_opt(default=False):
    """GeoJSON FeatureCollection output mode"""
    return click.option(
        '--collection',
        'geojson_type',
        flag_value='collection',
        default=default,
        help="Output as GeoJSON feature collection(s).")


def geojson_type_feature_opt(default=False):
    """GeoJSON Feature or Feature sequence output mode"""
    return click.option(
        '--feature',
        'geojson_type',
        flag_value='feature',
        default=default,
        help="Output as GeoJSON feature(s).")


def geojson_type_bbox_opt(default=False):
    """GeoJSON bbox output mode"""
    return click.option(
        '--bbox',
        'geojson_type',
        flag_value='bbox',
        default=default,
        help="Output as GeoJSON bounding box array(s).")

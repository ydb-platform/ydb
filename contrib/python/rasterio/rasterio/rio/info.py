"""Command access to dataset metadata, stats, and more."""


import json

from attr import asdict
import click

import rasterio
from rasterio.rio import options
from rasterio.transform import from_gcps


@click.command(short_help="Print information about a data file.")
@options.file_in_arg
@click.option('--meta', 'aspect', flag_value='meta', default=True,
              help="Show data file structure (default).")
@click.option('--tags', 'aspect', flag_value='tags',
              help="Show data file tags.")
@click.option('--namespace', help="Select a tag namespace.")
@click.option('--indent', default=None, type=int,
              help="Indentation level for pretty printed output")
# Options to pick out a single metadata item and print it as
# a string.
@click.option('--count', 'meta_member', flag_value='count',
              help="Print the count of bands.")
@click.option('-t', '--dtype', 'meta_member', flag_value='dtype',
              help="Print the dtype name.")
@click.option('--nodata', 'meta_member', flag_value='nodata',
              help="Print the nodata value.")
@click.option('-f', '--format', '--driver', 'meta_member', flag_value='driver',
              help="Print the format driver.")
@click.option('--shape', 'meta_member', flag_value='shape',
              help="Print the (height, width) shape.")
@click.option('--height', 'meta_member', flag_value='height',
              help="Print the height (number of rows).")
@click.option('--width', 'meta_member', flag_value='width',
              help="Print the width (number of columns).")
@click.option('--crs', 'meta_member', flag_value='crs',
              help="Print the CRS as a PROJ.4 string.")
@click.option('--bounds', 'meta_member', flag_value='bounds',
              help="Print the boundary coordinates "
                   "(left, bottom, right, top).")
@click.option('-r', '--res', 'meta_member', flag_value='res',
              help="Print pixel width and height.")
@click.option('--lnglat', 'meta_member', flag_value='lnglat',
              help="Print longitude and latitude at center.")
@click.option('--stats', 'meta_member', flag_value='stats',
              help="Print statistics (min, max, mean) of a single band "
                   "(use --bidx).")
@click.option('--checksum', 'meta_member', flag_value='checksum',
              help="Print integer checksum of a single band "
                   "(use --bidx).")
@click.option('--subdatasets', 'meta_member', flag_value='subdatasets',
              help="Print subdataset identifiers.")
@click.option('-v', '--tell-me-more', '--verbose', 'verbose', is_flag=True,
              help="Output extra information.")
@options.bidx_opt
@options.masked_opt
@click.pass_context
def info(ctx, input, aspect, indent, namespace, meta_member, verbose, bidx,
         masked):
    """Print metadata about the dataset as JSON.

    Optionally print a single metadata item as a string.
    """
    with ctx.obj['env'], rasterio.open(input) as src:

        info = dict(src.profile)
        info['shape'] = (info['height'], info['width'])
        info['bounds'] = src.bounds

        if src.crs:
            epsg = src.crs.to_epsg()
            if epsg:
                info["crs"] = f"EPSG:{epsg}"
            else:
                info['crs'] = src.crs.to_string()
        else:
            info['crs'] = None

        info['res'] = src.res
        info['colorinterp'] = [ci.name for ci in src.colorinterp]
        info['units'] = [units or None for units in src.units]
        info['descriptions'] = src.descriptions
        info['indexes'] = src.indexes
        info['mask_flags'] = [[
            flag.name for flag in flags] for flags in src.mask_flag_enums]

        if src.crs:
            info['lnglat'] = src.lnglat()

        gcps, gcps_crs = src.gcps

        if gcps:
            info['gcps'] = {'points': [p.asdict() for p in gcps]}
            if gcps_crs:
                epsg = gcps_crs.to_epsg()
                if epsg:
                    info["gcps"]["crs"] = f"EPSG:{epsg}"
                else:
                    info['gcps']['crs'] = src.crs.to_string()
            else:
                info['gcps']['crs'] = None

            info['gcps']['transform'] = from_gcps(gcps)

        if verbose:
            stats = [asdict(so) for so in src.stats()]
            info['stats'] = stats
            info['checksum'] = [src.checksum(i) for i in src.indexes]

        if aspect == 'meta':
            if meta_member == 'subdatasets':
                for name in src.subdatasets:
                    click.echo(name)
            elif meta_member == 'stats':
                st = src.stats(indexes=bidx)[0]
                click.echo("{st.min} {st.max} {st.mean} {st.std}".format(st=st))
            elif meta_member == 'checksum':
                click.echo(str(src.checksum(bidx)))
            elif meta_member:
                if isinstance(info[meta_member], (list, tuple)):
                    click.echo(" ".join(map(str, info[meta_member])))
                else:
                    click.echo(info[meta_member])
            else:
                click.echo(json.dumps(info, sort_keys=True, indent=indent))

        elif aspect == 'tags':
            click.echo(
                json.dumps(src.tags(ns=namespace), indent=indent))

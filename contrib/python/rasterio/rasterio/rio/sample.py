import json

import click

import rasterio


@click.command(short_help="Sample a dataset.")
@click.argument('files', nargs=-1, required=True, metavar='FILE "[x, y]"')
@click.option('-b', '--bidx', default=None, help="Indexes of input file bands.")
@click.pass_context
def sample(ctx, files, bidx):
    """Sample a dataset at one or more points

    Sampling points (x, y) encoded as JSON arrays, in the coordinate
    reference system of the dataset, are read from the second
    positional argument or stdin. Values of the dataset's bands
    are also encoded as JSON arrays and are written to stdout.

    Example:

    \b
        $ cat << EOF | rio sample tests/data/RGB.byte.tif
        > [220650, 2719200]
        > [219650, 2718200]
        > EOF
        [28, 29, 27]
        [25, 29, 19]

    By default, rio-sample will sample all bands. Optionally, bands
    may be specified using a simple syntax:

      --bidx N samples the Nth band (first band is 1).

      --bidx M,N,0 samples bands M, N, and O.

      --bidx M..O samples bands M-O, inclusive.

      --bidx ..N samples all bands up to and including N.

      --bidx N.. samples all bands from N to the end.

    Example:

    \b
        $ cat << EOF | rio sample tests/data/RGB.byte.tif --bidx ..2
        > [220650, 2719200]
        > [219650, 2718200]
        > EOF
        [28, 29]
        [25, 29]

    """
    files = list(files)
    source_path = files.pop(0)
    input = files.pop(0) if files else '-'

    # Handle the case of file, stream, or string input.
    try:
        points = click.open_file(input).readlines()
    except OSError:
        points = [input]

    with ctx.obj["env"]:
        with rasterio.open(source_path) as src:
            if bidx is None:
                indexes = src.indexes
            elif ".." in bidx:
                start, stop = map(lambda x: int(x) if x else None, bidx.split(".."))
                if start is None:
                    start = 1
                indexes = src.indexes[slice(start - 1, stop)]
            else:
                indexes = list(map(int, bidx.split(",")))

            for vals in src.sample(
                (json.loads(line) for line in points), indexes=indexes
            ):
                click.echo(json.dumps(vals.tolist()))

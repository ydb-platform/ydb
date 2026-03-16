"""$ rio calc"""

from collections import OrderedDict, UserDict
from contextlib import ExitStack
import math
from collections.abc import Mapping

import click
import numpy

import rasterio
from rasterio._vendor import snuggs
from rasterio.features import sieve
from rasterio.fill import fillnodata
from rasterio.rio import options
from rasterio.rio.helpers import resolve_inout
from rasterio.windows import Window, subdivide


def _get_bands(inputs, sources, d, i=None):
    """Get a rasterio.Band object from calc's inputs."""
    idx = d if d in dict(inputs) else int(d) - 1
    src = sources[idx]
    return rasterio.band(src, i) if i else [rasterio.band(src, j) for j in src.indexes]


def _read_array(ix, subix=None, dtype=None):
    """Change the type of a read array."""
    arr = snuggs._ctx.lookup(ix, subix)
    if dtype:
        arr = arr.astype(dtype)
    return arr


def asarray(*args):
    if len(args) == 1 and hasattr(args[0], "__iter__"):
        return numpy.asanyarray(list(args[0]))
    else:
        return numpy.asanyarray(list(args))


class FuncMapper(UserDict, Mapping):
    """Resolves functions from names in pipeline expressions."""

    def __getitem__(self, key):
        """Get a function by its name."""
        if key in self.data:
            return self.data[key]
        elif key in __builtins__ and not key.startswith("__"):
            return __builtins__[key]
        else:
            return (
                lambda g, *args, **kwargs: getattr(g, key)(*args, **kwargs)
                if callable(getattr(g, key))
                else getattr(g, key)
            )


@click.command(short_help="Raster data calculator.")
@click.argument('command')
@options.files_inout_arg
@options.output_opt
@options.format_opt
@click.option('--name', multiple=True,
              help='Specify an input file with a unique short (alphas only) '
                   'name for use in commands like '
                   '"a=tests/data/RGB.byte.tif".')
@options.dtype_opt
@options.masked_opt
@options.overwrite_opt
@click.option("--mem-limit", type=int, default=64, help="Limit on memory used to perform calculations, in MB.")
@options.creation_options
@click.pass_context
def calc(ctx, command, files, output, driver, name, dtype, masked, overwrite, mem_limit, creation_options):
    """A raster data calculator

    Evaluates an expression using input datasets and writes the result
    to a new dataset.

    Command syntax is lisp-like. An expression consists of an operator
    or function name and one or more strings, numbers, or expressions
    enclosed in parentheses. Functions include ``read`` (gets a raster
    array) and ``asarray`` (makes a 3-D array from 2-D arrays).

    \b
        * (read i) evaluates to the i-th input dataset (a 3-D array).
        * (read i j) evaluates to the j-th band of the i-th dataset (a
          2-D array).
        * (read i j 'float64') casts the array to, e.g. float64. This
          is critical if calculations will produces values that exceed
          the limits of the dataset's natural data type.
        * (take foo j) evaluates to the j-th band of a dataset named foo
          (see help on the --name option above).
        * Standard numpy array operators (+, -, \*, /) are available.
        * When the final result is a list of arrays, a multiple band
          output file is written.
        * When the final result is a single array, a single band output
          file is written.

    Example:

    \b
         $ rio calc "(+ 2 (* 0.95 (read 1)))" tests/data/RGB.byte.tif \\
         > /tmp/out.tif

    The command above produces a 3-band GeoTIFF with all values scaled
    by 0.95 and incremented by 2.

    \b
        $ rio calc "(asarray (+ 125 (read 1)) (read 1) (read 1))" \\
        > tests/data/shade.tif /tmp/out.tif

    The command above produces a 3-band RGB GeoTIFF, with red levels
    incremented by 125, from the single-band input.

    The maximum amount of memory used to perform calculations defaults to
    64 MB. This number can be increased to improve speed of calculation.

    """  # noqa: W605
    dst = None
    sources = []

    with ctx.obj["env"], ExitStack() as stack:
        output, files = resolve_inout(files=files, output=output, overwrite=overwrite)
        inputs = [tuple(n.split("=")) for n in name] + [(None, n) for n in files]
        sources = [stack.enter_context(rasterio.open(path)) for name, path in inputs]

        snuggs.func_map = FuncMapper(
            asarray=asarray,
            take=lambda a, idx: numpy.take(a, idx - 1, axis=0),
            read=_read_array,
            band=lambda d, i: _get_bands(inputs, sources, d, i),
            bands=lambda d: _get_bands(inputs, sources, d),
            fillnodata=fillnodata,
            sieve=sieve,
        )

        first = sources[0]
        kwargs = first.profile
        kwargs.update(**creation_options)
        dtype = dtype or first.meta["dtype"]
        kwargs["dtype"] = dtype
        kwargs.pop("driver", None)
        if driver:
            kwargs["driver"] = driver

        # The windows iterator is initialized with a single sample.
        # The actual work windows will be added in the second
        # iteration of the loop.
        work_windows = [Window(0, 0, 16, 16)]

        for window in work_windows:
            ctxkwds = OrderedDict()

            for i, ((name, path), src) in enumerate(zip(inputs, sources)):
                ctxkwds[name or "_i%d" % (i + 1)] = src.read(
                    masked=masked, window=window
                )

            try:
                res = snuggs.eval(command, **ctxkwds)
            except snuggs.ExpressionError as err:
                click.echo("Expression Error:")
                click.echo(f"  {err.text}")
                click.echo(" {}^".format(" " * err.offset))
                click.echo(err)
                raise click.Abort()
            else:
                results = res.astype(dtype)

            if isinstance(results, numpy.ma.core.MaskedArray):
                results = results.filled(float(kwargs["nodata"]))
                if len(results.shape) == 2:
                    results = numpy.ma.asanyarray([results])
            elif len(results.shape) == 2:
                results = numpy.asanyarray([results])

            # The first iteration is only to get sample results and from them
            # compute some properties of the output dataset.
            if dst is None:
                kwargs["count"] = results.shape[0]
                dst = stack.enter_context(rasterio.open(output, "w", **kwargs))
                max_pixels = mem_limit * 1.0e+6 / (numpy.dtype(dst.dtypes[0]).itemsize * dst.count)
                chunk_size = int(math.floor(math.sqrt(max_pixels)))
                work_windows.extend(
                    subdivide(
                        Window(0, 0, dst.width, dst.height),
                        chunk_size,
                        chunk_size
                    )
                )

            # In subsequent iterations we write results.
            else:
                dst.write(results, window=window)

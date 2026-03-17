#! /usr/bin/env python
"""python -m holoviews.util.command Conversion_Example.ipynb
OR
holoviews Conversion_Example.ipynb

"""

import argparse
import os
import sys
from argparse import RawTextHelpFormatter

try:
    import nbconvert
    import nbformat
except ImportError:
    print('nbformat, nbconvert and ipython need to be installed to use the holoviews command')
    sys.exit()
try:
    from ..ipython.preprocessors import (
        OptsMagicProcessor,
        OutputMagicProcessor,
        StripMagicsProcessor,
    )
except ImportError:
    from holoviews.ipython.preprocessors import (
        OptsMagicProcessor,
        OutputMagicProcessor,
        StripMagicsProcessor,
    )

from . import examples

_PREPROCESSORS = [OptsMagicProcessor(), OutputMagicProcessor(), StripMagicsProcessor()]

def main():
    if len(sys.argv) < 2:
        print("For help with the holoviews command run:\n\nholoviews --help\n")
        sys.exit()

    parser = argparse.ArgumentParser(prog='holoviews',
                                     formatter_class=RawTextHelpFormatter,
                                     description=description,
                                     epilog=epilog)

    parser.add_argument('--notebook', metavar='notebook', type=str, nargs=1,
                    help='The Jupyter notebook to convert to Python syntax.')

    parser.add_argument('--install-examples', metavar='install_examples',
                        type=str, nargs='?',
                        help='Install examples to the specified directory.')

    args = parser.parse_args()
    if args.notebook:
        print(export_to_python(args.notebook[0]), file=sys.stdout)
    else:
        if args.install_examples is None:
            examples_dir = 'holoviews-examples'
        else:
            examples_dir = args.install_examples
        curdir,_ = os.path.split(__file__)
        root = os.path.abspath(os.path.join('..','..', curdir))
        examples(path=examples_dir, root=root)


def export_to_python(filename=None, preprocessors=None):
    if preprocessors is None:
        preprocessors = _PREPROCESSORS.copy()
    filename = filename if filename else sys.argv[1]
    with open(filename) as f:
        nb = nbformat.read(f, nbformat.NO_CONVERT)
        exporter = nbconvert.PythonExporter()
        for preprocessor in preprocessors:
            exporter.register_preprocessor(preprocessor)
        source, _meta = exporter.from_notebook_node(nb)
        return source



description = """
Command line interface for holoviews.

This utility allows conversion of notebooks containing the HoloViews
%opts, %%opts, %output and %%output magics to regular Python
syntax. This is useful for turning Jupyter notebooks using HoloViews
into Bokeh applications that can be served with:

bokeh server --show converted_notebook.py

The holoviews command supports the following options:
"""

epilog="""
Example usage
-------------

$ holoviews ./examples/demos/matplotlib/area_chart.ipynb

The converted syntax is then output to standard output where you can
direct it to a Python file of your choosing.
"""


if __name__ == '__main__':
    main()

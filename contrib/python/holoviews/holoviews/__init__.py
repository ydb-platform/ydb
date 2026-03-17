"""
HoloViews makes data analysis and visualization simple
======================================================

HoloViews lets you focus on what you are trying to explore and convey, not on
the process of plotting.

HoloViews

- supports a wide range of data sources including Pandas, Dask, XArray Rapids cuDF, Streamz, Intake, Geopandas, NetworkX and Ibis.

- supports the plotting backends Bokeh (default), Matplotlib and Plotly.

- allows you to drop into the rest of the HoloViz ecosystem when more power or flexibility is needed.

For basic data exploration we recommend using the higher level hvPlot package,
which provides the familiar Pandas `.plot` api. You can drop into HoloViews
when needed.

To learn more check out https://holoviews.org/. To report issues or contribute
go to https://github.com/holoviz/holoviews. To join the community go to
https://discourse.holoviz.org/.

How to use HoloViews in 3 simple steps
--------------------------------------

Work with the data source you already know and ❤️

>>> import pandas as pd
>>> station_info = pd.read_csv('https://raw.githubusercontent.com/holoviz/holoviews/main/examples/assets/station_info.csv')

Import HoloViews and configure your plotting backend

>>> import holoviews as hv
>>> hv.extension('bokeh')

Annotate your data

>>> scatter = (
...     hv.Scatter(station_info, kdims='services', vdims='ridership')
...     .redim(
...         services=hv.Dimension("services", label='Services'),
...         ridership=hv.Dimension("ridership", label='Ridership'),
...     )
...     .opts(size=10, color="red", responsive=True)
... )
>>> scatter

In a notebook this will display a nice scatter plot.

Note that the `kdims` (The key dimension(s)) represents the independent
variable(s) and the `vdims` (value dimension(s)) the dependent variable(s).

For more check out https://holoviews.org/getting_started/Introduction.html

How to get help
---------------

You can understand the structure of your objects by printing them.

>>> print(scatter)
:Scatter   [services]   (ridership)

You can get extensive documentation using `hv.help`.

>>> hv.help(scatter)

In a notebook or ipython environment the usual

- `help` and `?` will provide you with documentation.
- `TAB` and `SHIFT+TAB` completion will help you navigate.

To ask the community go to https://discourse.holoviz.org/.
To report issues go to https://github.com/holoviz/holoviews.

"""
import builtins
import os
import sys
from typing import TYPE_CHECKING

import param

from . import util  # noqa (API import)
from .__version import __version__
from .core import archive, config  # noqa (API import)
from .core.boundingregion import BoundingBox  # noqa (API import)
from .core.dimension import Dimension  # noqa (API import)
from .core.element import Collator, Element  # noqa (API import)
from .core.layout import AdjointLayout, Empty, Layout, NdLayout  # noqa (API import)
from .core.ndmapping import NdMapping  # noqa (API import)
from .core.options import (  # noqa (API import)
    Cycle,
    Options,
    Palette,
    Store,
    StoreOptions,
)
from .core.overlay import NdOverlay, Overlay  # noqa (API import)
from .core.spaces import (  # noqa (API import)
    Callable,
    DynamicMap,
    GridMatrix,
    GridSpace,
    HoloMap,
)
from .element import *
from .element import __all__ as elements_list
from .operation import Operation  # noqa (API import)
from .selection import link_selections  # noqa (API import)
from .util import (  # noqa (API import)
    _load_rc_file,
    extension,
    opts,
    output,
    render,
    renderer,
    save,
)
from .util._versions import show_versions  # noqa: F401
from .util.transform import dim  # noqa (API import)
from .util.warnings import (  # noqa: F401
    HoloviewsDeprecationWarning,
    HoloviewsUserWarning,
)

if hasattr(builtins, "__IPYTHON__"):
    from .ipython import notebook_extension
    extension = notebook_extension
else:
    class notebook_extension(param.ParameterizedFunction):
        def __call__(self, *args, **kwargs):
            raise Exception("Jupyter notebook not available: use hv.extension instead.")

if '_pyodide' in sys.modules:
    from .pyodide import in_jupyterlite, pyodide_extension
    # The notebook_extension is needed inside jupyterlite,
    # so the override is only done if we are not inside jupyterlite.
    if in_jupyterlite():
        extension.inline = False
    else:
        extension = pyodide_extension
    del pyodide_extension, in_jupyterlite


if TYPE_CHECKING:
    # Adding this here to have better docstring in LSP
    from .util import extension

# A single holoviews.rc file may be executed if found.
# In HoloViews 1.23.0, it will need to be set with env. var. HOLOVIEWSRC
_load_rc_file()

def help(obj, visualization=True, ansi=True, backend=None,
         recursive=False, pattern=None):
    """Extended version of the built-in help that supports parameterized
    functions and objects. A pattern (regular expression) may be used to
    filter the output and if recursive is set to True, documentation for
    the supplied object is shown. Note that the recursive option will
    only work with an object instance and not a class.

    If ``ansi`` is set to False, all ANSI color codes are stripped out.

    """
    backend = backend if backend else Store.current_backend
    info = Store.info(obj, ansi=ansi, backend=backend, visualization=visualization,
                      recursive=recursive, pattern=pattern, elements=elements_list)

    msg = ("\nTo view the visualization options applicable to this "
           "object or class, use:\n\n"
           "   holoviews.help(obj, visualization=True)\n\n")
    if info:
        print((msg if visualization is False else '') + info)
    else:
        import pydoc
        pydoc.help(obj)


del builtins, os, sys, _load_rc_file

def __getattr__(name):
    if name == "annotate":
        # Lazy loading Panel
        from .annotators import annotate
        return annotate
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [k for k in locals() if not k.startswith('_')]
__all__ += ['__version__', 'annotate']

def __dir__():
    return __all__

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .annotators import annotate

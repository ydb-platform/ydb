import os
import platform
import sys
from importlib.metadata import version

__all__ = ("show_versions",)

PACKAGES = [
    # Data
    "cudf",
    "cupy",
    "dask",
    "dask-expr",
    "duckdb",
    "ibis-framework",
    "narwhals",
    "networkx",
    "numpy",
    "pandas",
    "polars",
    "pyarrow",
    "spatialpandas",
    "xarray",
    # Processing
    "numba",
    "scikit-image",
    "scipy",
    "tsdownsample",
    # Plotting
    "bokeh",
    "colorcet",
    "datashader",
    "geoviews",
    "hvplot",
    "matplotlib",
    "pillow",
    "plotly",
    # Jupyter
    "IPython",
    "jupyter_bokeh",
    "ipywidgets_bokeh",
    "jupyterlab",
    "notebook",
    # Misc
    "panel",
    "param",
    "pyviz_comms",
]


def show_versions():
    print(f"Python              :  {sys.version}")
    print(f"Operating system    :  {platform.platform()}")
    _panel_comms()
    _hv_rc_file()
    print()
    _package_version("holoviews")
    print()
    for p in sorted(PACKAGES, key=lambda x: x.lower()):
        _package_version(p)


def _package_version(p):
    try:
        print(f"{p.replace('_', '-'):20}:  {version(p)}")
    except ImportError:
        print(f"{p.replace('_', '-'):20}:  -")


def _panel_comms():
    import panel as pn

    print(f"{'Panel comms':20}:  {pn.config.comms}")


def _hv_rc_file():
    rc_file = os.getenv('HOLOVIEWSRC')
    if rc_file:
        print(f"{'HoloViews config':20}:  {rc_file}")


if __name__ == "__main__":
    show_versions()

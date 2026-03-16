"""
Bokeh app example using datashader for rasterizing a large dataset and
geoviews for reprojecting coordinate systems.

This example requires the 1.7GB nyc_taxi_wide.parquet dataset which
you can obtain by downloading the file from AWS:

  https://s3.amazonaws.com/datashader-data/nyc_taxi_wide.parq

Place this parquet in a data/ subfolder and install the python dependencies, e.g.

  conda install datashader fastparquet python-snappy

You can now run this app with:

  bokeh serve --show nytaxi_hover.py

"""
import dask.dataframe as dd
import numpy as np

import holoviews as hv
from holoviews import opts
from holoviews.operation.datashader import aggregate

hv.extension('bokeh')
renderer = hv.renderer('bokeh')

# Set plot and style options
opts.defaults(
    opts.Curve(xaxis=None, yaxis=None, show_grid=False, show_frame=False,
               color='orangered', framewise=True, width=100),
    opts.Image(width=800, height=400, shared_axes=False, logz=True, colorbar=True,
               xaxis=None, yaxis=None, axiswise=True, bgcolor='black'),
    opts.HLine(color='white', line_width=1),
    opts.Layout(shared_axes=False),
    opts.VLine(color='white', line_width=1))

# Read the parquet file
df = dd.read_parquet('./data/nyc_taxi_wide.parq').persist()

# Declare points
points = hv.Points(df, kdims=['pickup_x', 'pickup_y'], vdims=[])

# Use datashader to rasterize and linked streams for interactivity
agg = aggregate(points, link_inputs=True, x_sampling=0.0001, y_sampling=0.0001)
pointerx = hv.streams.PointerX(x=np.mean(points.range('pickup_x')), source=points)
pointery = hv.streams.PointerY(y=np.mean(points.range('pickup_y')), source=points)
vline = hv.DynamicMap(lambda x: hv.VLine(x), streams=[pointerx])
hline = hv.DynamicMap(lambda y: hv.HLine(y), streams=[pointery])

sampled = hv.util.Dynamic(agg, operation=lambda obj, x: obj.sample(pickup_x=x),
                          streams=[pointerx], link_inputs=False)

hvobj = ((agg * hline * vline) << sampled)

# Obtain Bokeh document and set the title
doc = renderer.server_doc(hvobj)
doc.title = 'NYC Taxi Crosshair'

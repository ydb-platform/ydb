"""
An example of a minimal bokeh app which can be served with:

    bokeh serve --show sine

It defines a simple DynamicMap returning a Curve of a sine wave with
frequency and phase dimensions, which can be varied using sliders.
"""

import numpy as np

import holoviews as hv
import holoviews.plotting.bokeh

renderer = hv.renderer('bokeh')
xs = np.linspace(0, np.pi*2)
dmap = (hv.DynamicMap(lambda f, p: hv.Curve(np.sin(xs*f+p)), kdims=['f', 'p'])
    .redim(p=dict(range=(0, np.pi*2), step=0.1), f=dict(range=(1, 5), step=0.1)))

doc = renderer.server_doc(dmap)
doc.title = 'Sine Demo'

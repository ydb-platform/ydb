import numpy as np
import panel as pn
from scipy.signal import convolve2d

import holoviews as hv
from holoviews import opts
from holoviews.streams import Counter, DoubleTap, Tap

hv.extension('bokeh')

diehard = [[0, 0, 0, 0, 0, 0, 1, 0],
           [1, 1, 0, 0, 0, 0, 0, 0],
           [0, 1, 0, 0, 0, 1, 1, 1]]

boat = [[1, 1, 0],
        [1, 0, 1],
        [0, 1, 0]]

r_pentomino = [[0, 1, 1],
               [1, 1, 0],
               [0, 1, 0]]

beacon = [[0, 0, 1, 1],
          [0, 0, 1, 1],
          [1, 1, 0, 0],
          [1, 1, 0, 0]]

acorn = [[0, 1, 0, 0, 0, 0, 0],
         [0, 0, 0, 1, 0, 0, 0],
         [1, 1, 0, 0, 1, 1, 1]]

spaceship = [[0, 0, 1, 1, 0],
             [1, 1, 0, 1, 1],
             [1, 1, 1, 1, 0],
             [0, 1, 1, 0, 0]]

block_switch_engine = [[0, 0, 0, 0, 0, 0, 1, 0],
                       [0, 0, 0, 0, 1, 0, 1, 1],
                       [0, 0, 0, 0, 1, 0, 1, 0],
                       [0, 0, 0, 0, 1, 0, 0, 0],
                       [0, 0, 1, 0, 0, 0, 0, 0],
                       [1, 0, 1, 0, 0, 0, 0, 0]]

glider = [[1, 0, 0], [0, 1, 1], [1, 1, 0]]

unbounded = [[1, 1, 1, 0, 1],
            [1, 0, 0, 0, 0],
            [0, 0, 0, 1, 1],
            [0, 1, 1, 0, 1],
            [1, 0, 1, 0, 1]]

shapes = {'Glider': glider, 'Block Switch Engine': block_switch_engine,
          'Spaceship': spaceship, 'Acorn': acorn, 'Beacon': beacon,
          'Diehard': diehard, 'Unbounded': unbounded}

def step(X):
    nbrs_count = convolve2d(X, np.ones((3, 3)), mode='same', boundary='wrap') - X
    return (nbrs_count == 3) | (X & (nbrs_count == 2))

def update(pattern, counter, x, y):
    if x and y:
        pattern = np.array(shapes[pattern])
        r, c = pattern.shape
        y, x = img.sheet2matrixidx(x,y)
        img.data[y:y+r,x:x+c] = pattern[::-1]
    else:
        img.data = step(img.data)
    return hv.Image(img)

# Set up plot which advances on counter and adds pattern on tap
title = 'Game of Life - Tap to place pattern, Doubletap to clear'
img = hv.Image(np.zeros((100, 200), dtype=np.uint8))
counter, tap = Counter(transient=True), Tap(transient=True),
pattern_dim = hv.Dimension('Pattern', values=sorted(shapes.keys()))
dmap = hv.DynamicMap(update, kdims=[pattern_dim], streams=[counter, tap])

plot = dmap.opts(
    opts.Image(cmap='gray', clim=(0, 1), toolbar=None, responsive=True,
               min_height=800, title=title, xaxis=None, yaxis=None)
)

# Add callback to clear on double tap
def reset_data(x, y):
    img.data[:] = 0

reset = DoubleTap(transient=True, source=plot)
reset.add_subscriber(reset_data)

# Set up Panel app and periodic callback
panel = pn.pane.HoloViews(plot, center=True, widget_location='right')

def advance():
    counter.event(counter=counter.counter+1)
pn.state.add_periodic_callback(advance, period=50, start=False)

panel.servable('Game of Life')

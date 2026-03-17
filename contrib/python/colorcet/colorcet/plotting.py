"""
This module is not required for colorcet - it contains functions
to facilitate plotting of colormaps - and is mainly used in the
documentation.
"""

import numpy as np
import holoviews as hv
from holoviews import opts

from . import get_aliases, all_original_names, palette, cm
from .sineramp import sineramp

array = np.meshgrid(np.linspace(0, 1, 256), np.linspace(0, 1, 10))[0]

def swatch(name, cmap=None, bounds=None, array=array, **kwargs):
    """Show a color swatch for a colormap using matplotlib or bokeh via holoviews.
    Colormaps can be selected by `name`, including those in Colorcet
    along with any standard Bokeh palette or named Matplotlib colormap.

    Custom colormaps can be visualized by passing an explicit
    list of colors (for Bokeh) or the colormap object (for Matplotlib) to `cmap`.

    HoloViews options for either backend can be passed in as kwargs,
    so that you can customize the width, height, etc. of the swatch.

    The `bounds` and `array` arguments allow you to customize the
    portion of the colormap to show and how many samples to take
    from it; see the source code and hv.Image documentation for
    details.
    """
    title = name if cmap else get_aliases(name)
    if bounds is None:
        bounds = (0, 0, 256, 1)

    if type(cmap) is tuple:
        cmap = list(cmap)

    plot = hv.Image(array, bounds=bounds, group=title)
    backends = hv.Store.loaded_backends()
    if 'bokeh' in backends:
        width = kwargs.pop('width', 900)
        height = kwargs.pop('height', 100)
        plot.opts(opts.Image(backend='bokeh', width=width, height=height, toolbar='above',
                             default_tools=['xwheel_zoom', 'xpan', 'save', 'reset'],
                             cmap=cmap or palette[name]))
    if 'matplotlib' in backends:
        aspect = kwargs.pop('aspect', 15)
        fig_size = kwargs.pop('fig_size', 350)
        plot.opts(opts.Image(backend='matplotlib', aspect=aspect, fig_size=fig_size,
                             cmap=cmap or cm[name]))
    return plot.opts(opts.Image(xaxis=None, yaxis=None), opts.Image(**kwargs))

def swatches(*args, group=None, not_group=None, only_aliased=False, cols=None, **kwargs):
    """Show swatches for given names or names in group"""
    args = args or all_original_names(group=group, not_group=not_group,
                                      only_aliased=only_aliased)
    if not cols:
        cols = 3 if len(args) >= 3 else 1

    backends = hv.Store.loaded_backends()
    if 'matplotlib' in backends:
        if 'aspect' not in kwargs:
            kwargs['aspect'] = 12 // cols
        if 'fig_size' not in kwargs:
            kwargs['fig_size'] = 500 // cols
    if 'bokeh' in backends:
        if 'height' not in kwargs:
            kwargs['height'] = 100
        if 'width' not in kwargs:
            kwargs['width'] = (9 * kwargs['height']) // cols

    images = [swatch(arg, **kwargs) if isinstance(arg, str) else
              swatch(*arg, **kwargs) for
              arg in args]

    plot = hv.Layout(images).opts(transpose=True).cols(int(np.ceil(len(images)*1.0/cols)))

    if 'matplotlib' in backends:
        plot.opts(opts.Layout(backend='matplotlib', sublabel_format=None,
                              fig_size=kwargs.get('fig_size', 150)))
    return plot

sine = sineramp()

def sine_comb(name, cmap=None, **kwargs):
    """Show sine_comb using matplotlib or bokeh via holoviews"""
    title = name if cmap else get_aliases(name)
    plot = hv.Image(sine, group=title)

    backends = hv.Store.loaded_backends()
    if 'bokeh' in backends:
        plot.opts(opts.Image(backend='bokeh', width=400, height=150, toolbar='above',
                             cmap=cmap or palette[name]))
    if 'matplotlib' in backends:
        plot.opts(opts.Image(backend='matplotlib', aspect=3, fig_size=200,
                             cmap=cmap or cm[name]))

    return plot.opts(opts.Image(xaxis=None, yaxis=None), opts.Image(**kwargs))

def sine_combs(*args, group=None, not_group=None, only_aliased=False, cols=1, **kwargs):
    """Show sine_combs for given names or names in group"""
    args = args or all_original_names(group=group, not_group=not_group,
                                      only_aliased=only_aliased)
    images = [sine_comb(arg, **kwargs) if isinstance(arg, str) else
              sine_comb(*arg, **kwargs) for
              arg in args]

    plot = hv.Layout(images).opts(transpose=True).cols(int(np.ceil(len(images)*1.0/cols)))

    backends = hv.Store.loaded_backends()
    if 'matplotlib' in backends:
        plot.opts(opts.Layout(backend='matplotlib', sublabel_format=None,
                              fig_size=kwargs.get('fig_size', 200)))
    return plot

arr = np.arange(0, 100)
np.random.shuffle(arr)
zz = arr.reshape(10, 10)
xx, yy = np.meshgrid(np.arange(0,10), np.arange(0,10))

data = np.array([xx, yy, zz]).transpose().reshape(100, 3)

def candy_buttons(name, cmap=None, size=450, **kwargs):
    if cmap is None:
        cmap = palette[name][:100]
        name = get_aliases(name)
    options = opts.Points(color='color', size=size/13.0, tools=['hover'],
                          yaxis=None, xaxis=None, height=size, width=size,
                          cmap=cmap, **kwargs)
    return hv.Points(data, vdims='color').opts(options).relabel(name)

# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Define the SlippyImageArtist class, which interfaces with
:class:`cartopy.io.RasterSource` instances at draw time, for interactive
dragging and zooming of raster data.

"""

import matplotlib.artist
from matplotlib.image import AxesImage


class SlippyImageArtist(AxesImage):

    """
    A subclass of :class:`~matplotlib.image.AxesImage` which provides an
    interface for getting a raster from the given object with interactive
    slippy map type functionality.

    Kwargs are passed to the AxesImage constructor.

    """

    def __init__(self, ax, raster_source, **kwargs):
        self.raster_source = raster_source
        # This artist fills the Axes, so should not influence layout.
        kwargs.setdefault('in_layout', False)
        super().__init__(ax, **kwargs)
        self.cache = []

        ax.figure.canvas.mpl_connect('button_press_event', self.on_press)
        ax.figure.canvas.mpl_connect('button_release_event', self.on_release)

        self.on_release()

    def on_press(self, event=None):
        self.user_is_interacting = True

    def on_release(self, event=None):
        self.user_is_interacting = False
        self.stale = True

    def get_window_extent(self, renderer=None):
        return self.axes.get_window_extent(renderer=renderer)

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer, *args, **kwargs):
        if not self.get_visible():
            return

        ax = self.axes
        window_extent = ax.get_window_extent()
        [x1, y1], [x2, y2] = ax.viewLim.get_points()
        if not self.user_is_interacting:
            located_images = self.raster_source.fetch_raster(
                ax.projection, extent=[x1, x2, y1, y2],
                target_resolution=(window_extent.width, window_extent.height))
            self.cache = located_images

        for img, extent in self.cache:
            self.set_array(img)
            with ax.hold_limits():
                self.set_extent(extent)
            super().draw(renderer, *args, **kwargs)

    def can_composite(self):
        # As per https://github.com/SciTools/cartopy/issues/689, disable
        # compositing multiple raster sources.
        return False

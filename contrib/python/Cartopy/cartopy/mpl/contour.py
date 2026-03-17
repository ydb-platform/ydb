# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from matplotlib.contour import QuadContourSet
import matplotlib.path as mpath
import numpy as np

from cartopy.mpl import _MPL_38


class GeoContourSet(QuadContourSet):
    """
    A contourset designed to handle things like contour labels.

    """
    # nb. No __init__ method here - most of the time a GeoContourSet will
    # come from GeoAxes.contour[f]. These methods morph a ContourSet by
    # fiddling with instance.__class__.

    def clabel(self, *args, **kwargs):
        if not _MPL_38:
            # nb: contour labelling does not work very well for filled
            # contours - it is recommended to only label line contours.
            # This is especially true when inline=True.

            # This wrapper exist because mpl does not properly transform
            # paths. Instead it simply assumes one path represents one polygon
            # (not necessarily the case), and it assumes that
            # transform(path.verts) is equivalent to transform_path(path).
            # Unfortunately there is no way to easily correct this error,
            # so we are forced to pre-transform the ContourSet's paths from
            # the source coordinate system to the axes' projection.
            # The existing mpl code then has a much simpler job of handling
            # pre-projected paths (which can now effectively be transformed
            # naively).

            for col in self.collections:
                # Snaffle the collection's path list. We will change the
                # list in-place (as the contour label code does in mpl).
                paths = col.get_paths()

                # Define the transform that will take us from collection
                # coordinates through to axes projection coordinates.
                data_t = self.axes.transData
                col_to_data = col.get_transform() - data_t

                # Now that we have the transform, project all of this
                # collection's paths.
                new_paths = [col_to_data.transform_path(path)
                             for path in paths]
                new_paths = [path for path in new_paths
                             if path.vertices.size >= 1]

                # The collection will now be referenced in axes projection
                # coordinates.
                col.set_transform(data_t)

                # Clear the now incorrectly referenced paths.
                del paths[:]

                for path in new_paths:
                    if path.vertices.size == 0:
                        # Don't persist empty paths. Let's get rid of them.
                        continue

                    # Split the path if it has multiple MOVETO statements.
                    codes = np.array(
                        path.codes if path.codes is not None else [0])
                    moveto = codes == mpath.Path.MOVETO
                    if moveto.sum() <= 1:
                        # This is only one path, so add it to the collection.
                        paths.append(path)
                    else:
                        # The first MOVETO doesn't need cutting-out.
                        moveto[0] = False
                        split_locs = np.flatnonzero(moveto)

                        split_verts = np.split(path.vertices, split_locs)
                        split_codes = np.split(path.codes, split_locs)

                        for verts, codes in zip(split_verts, split_codes):
                            # Add this path to the collection's list of paths.
                            paths.append(mpath.Path(verts, codes))

        else:
            # Where contour paths exist at the edge of the globe, sometimes a
            # complete path in data space will become multiple paths when
            # transformed into axes or screen space.  Matplotlib's contour
            # labelling does not account for this so we need to give it the
            # pre-transformed paths to work with.

            # Define the transform that will take us from collection
            # coordinates through to axes projection coordinates.
            data_t = self.axes.transData
            col_to_data = self.get_transform() - data_t

            # Now that we have the transform, project all of this
            # collection's paths.
            paths = self.get_paths()
            new_paths = [col_to_data.transform_path(path) for path in paths]
            self.set_paths(new_paths)

            # The collection will now be referenced in axes projection
            # coordinates.
            self.set_transform(data_t)

        # Now that we have prepared the collection paths, call on
        # through to the underlying implementation.
        return super().clabel(*args, **kwargs)

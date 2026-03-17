# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
from matplotlib.collections import QuadMesh
import numpy as np
import numpy.ma as ma

from cartopy.mpl import _MPL_38


def _split_wrapped_mesh_data(C, mask):
    """
    Helper function for splitting GeoQuadMesh array values between the
    pcolormesh and pcolor objects when wrapping.  Apply a mask to the grid
    cells that should not be plotted with each method.

    """
    # The original data mask (regardless of wrapped cells)
    C_mask = getattr(C, 'mask', None)
    if C.ndim == 3:
        # RGB(A) array.
        if not _MPL_38:
            raise ValueError("GeoQuadMesh wrapping for RGB(A) requires "
                             "Matplotlib v3.8 or later")

        # mask will need an extra trailing dimension
        mask = np.broadcast_to(mask[..., np.newaxis], C.shape)

    # create the masked array to be used with pcolormesh
    full_mask = mask if C_mask is None else mask | C_mask
    pcolormesh_data = ma.array(C, mask=full_mask)

    # create the masked array to be used with pcolor
    full_mask = ~mask if C_mask is None else ~mask | C_mask
    pcolor_data = ma.array(C, mask=full_mask)

    return pcolormesh_data, pcolor_data, ~mask


class GeoQuadMesh(QuadMesh):
    """
    A QuadMesh designed to help handle the case when the mesh is wrapped.

    """
    # No __init__ method here - most of the time a GeoQuadMesh will
    # come from GeoAxes.pcolormesh. These methods morph a QuadMesh by
    # fiddling with instance.__class__.

    def get_array(self):
        # Retrieve the array - use copy to avoid any chance of overwrite
        A = super().get_array().copy()
        # If the input array has a mask, retrieve the associated data
        if hasattr(self, '_wrapped_mask'):
            pcolor_data = self._wrapped_collection_fix.get_array()
            mask = self._wrapped_mask
            if not _MPL_38:
                A[mask] = pcolor_data
            else:
                if A.ndim == 3:  # RGB(A) data.  Need to broadcast mask.
                    mask = mask[:, :, np.newaxis]
                # np.copyto is not implemented for masked arrays so handle the
                # mask explicitly
                np.copyto(A.mask, pcolor_data.mask, where=mask)
                np.copyto(A, pcolor_data, where=mask)

        return A

    def set_array(self, A):
        # Check the shape is appropriate up front.
        if not _MPL_38:
            # Need to figure out existing shape from the coordinates.
            height, width = self._coordinates.shape[0:-1]
            if self._shading == 'flat':
                h, w = height - 1, width - 1
            else:
                h, w = height, width
        else:
            h, w = super().get_array().shape[:2]

        ok_shapes = [(h, w, 3), (h, w, 4), (h, w), (h * w,)]
        if A.shape not in ok_shapes:
            ok_shape_str = ' or '.join(map(str, ok_shapes))
            raise ValueError(
                f"A should have shape {ok_shape_str}, not {A.shape}")

        if A.ndim == 1:
            # Always use array with at least two dimensions.  This is
            # inconsistent with QuadMesh which stores whatever you give it, but
            # for the wrapped case we need to match the 2D mask.  Storing the
            # 2D array also allows us to calculate ok_shapes on subsequent
            # calls without using the private QuadMesh._shading attribute.
            A = A.reshape((h, w))

        # Only use the mask attribute if it is there.
        if hasattr(self, '_wrapped_mask'):

            # Update the pcolor data with the wrapped masked data
            A, pcolor_data, _ = _split_wrapped_mesh_data(A, self._wrapped_mask)

            if not _MPL_38:
                self._wrapped_collection_fix.set_array(
                    pcolor_data[self._wrapped_mask].ravel())
            else:
                self._wrapped_collection_fix.set_array(pcolor_data)

        # Now that we have prepared the collection data, call on
        # through to the underlying implementation.
        super().set_array(A)

    def set_clim(self, vmin=None, vmax=None):
        # Update _wrapped_collection_fix color limits if it is there.
        if hasattr(self, '_wrapped_collection_fix'):
            self._wrapped_collection_fix.set_clim(vmin, vmax)

        # Update color limits for the rest of the cells.
        super().set_clim(vmin, vmax)

    def get_datalim(self, transData):
        # Return the corners that were calculated in
        # the pcolormesh routine.
        return self._corners

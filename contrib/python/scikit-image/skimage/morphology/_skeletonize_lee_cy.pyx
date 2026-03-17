"""
This is an implementation of the 2D/3D thinning algorithm
of [Lee94]_ of binary images, based on [IAC15]_.

The original Java code [IAC15]_ carries the following message:

 * This work is an implementation by Ignacio Arganda-Carreras of the
 * 3D thinning algorithm from Lee et al. "Building skeleton models via 3-D
 * medial surface/axis thinning algorithms. Computer Vision, Graphics, and
 * Image Processing, 56(6):462-478, 1994." Based on the ITK version from
 * Hanno Homann <a href="http://hdl.handle.net/1926/1292"> http://hdl.handle.net/1926/1292</a>
 * <p>
 *  More information at Skeletonize3D homepage:
 *  https://imagej.net/Skeletonize3D
 *
 * @version 1.0 11/13/2015 (unique BSD licensed version for scikit-image)
 * @author Ignacio Arganda-Carreras (iargandacarreras at gmail.com)

References
----------
.. [Lee94] T.-C. Lee, R.L. Kashyap and C.-N. Chu, Building skeleton models
       via 3-D medial surface/axis thinning algorithms.
       Computer Vision, Graphics, and Image Processing, 56(6):462-478, 1994.

.. [IAC15] Ignacio Arganda-Carreras, 2015. Skeletonize3D plugin for ImageJ(C).
           https://imagej.net/Skeletonize3D

"""

from libc.string cimport memcpy
from libcpp.vector cimport vector

import numpy as np
from numpy cimport npy_intp, npy_uint8, ndarray
cimport cython

ctypedef npy_uint8 pixel_type

# struct to hold 3D coordinates
cdef struct coordinate:
    npy_intp p
    npy_intp r
    npy_intp c


@cython.boundscheck(False)
@cython.wraparound(False)
def _compute_thin_image(pixel_type[:, :, ::1] img not None):
    """Compute a thin image.

    Loop through the image multiple times, removing "simple" points, i.e.
    those point which can be removed without changing local connectivity in the
    3x3x3 neighborhood of a point.

    This routine implements the two-pass algorithm of [Lee94]_. Namely,
    for each of the six border types (positive and negative x-, y- and z-),
    the algorithm first collects all possibly deletable points, and then
    performs a sequential rechecking.

    The input, `img`, is assumed to be a 3D binary image in the
    (p, r, c) format [i.e., C ordered array], filled by zeros (background) and
    ones. Furthermore, `img` is assumed to be padded by zeros from all
    directions --- this way the zero boundary conditions are automatic
    and there is need to guard against out-of-bounds access.

    """
    cdef:
        int unchanged_borders = 0, curr_border, num_borders
        int borders[6]
        npy_intp p, r, c
        bint no_change

        # list simple_border_points
        vector[coordinate] simple_border_points
        coordinate point

        Py_ssize_t num_border_points, i, j

        pixel_type neighb[27]

    # loop over the six directions in this order (for consistency with ImageJ)
    borders[:] = [4, 3, 2, 1, 5, 6]

    with nogil:
        # no need to worry about the z direction if the original image is 2D.
        if img.shape[0] == 3:
            num_borders = 4
        else:
            num_borders = 6

        # loop through the image several times until there is no change for all
        # the six border types
        while unchanged_borders < num_borders:
            unchanged_borders = 0
            for j in range(num_borders):
                curr_border = borders[j]

                find_simple_point_candidates(img, curr_border, simple_border_points)

                # sequential re-checking to preserve connectivity when deleting
                # in a parallel way
                no_change = True
                num_border_points = simple_border_points.size()
                for i in range(num_border_points):
                    point = simple_border_points[i]
                    p = point.p
                    r = point.r
                    c = point.c
                    get_neighborhood(img, p, r, c, neighb)
                    if is_simple_point(neighb):
                        img[p, r, c] = 0
                        no_change = False

                if no_change:
                    unchanged_borders += 1

    return np.asarray(img)


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void find_simple_point_candidates(pixel_type[:, :, ::1] img,
                                       int curr_border,
                                       vector[coordinate] & simple_border_points) noexcept nogil:
    """Inner loop of compute_thin_image.

    The algorithm of [Lee94]_ proceeds in two steps: (1) six directions are
    checked for simple border points to remove, and (2) these candidates are
    sequentially rechecked, see Sec 3 of [Lee94]_ for rationale and discussion.

    This routine implements the first step above: it loops over the image
    for a given direction and assembles candidates for removal.

    """
    cdef:
        cdef coordinate point

        pixel_type neighborhood[27]
        npy_intp p, r, c
        bint is_border_pt

        # rebind a global name to avoid lookup. The table is filled in
        # at import time.
        int[::1] Euler_LUT = LUT

    # clear the output vector
    simple_border_points.clear();

    # loop through the image
    # NB: each loop is from 1 to size-1: img is padded from all sides
    for p in range(1, img.shape[0] - 1):
        for r in range(1, img.shape[1] - 1):
            for c in range(1, img.shape[2] - 1):

                # check if pixel is foreground
                if img[p, r, c] != 1:
                    continue

                is_border_pt = (curr_border == 1 and img[p, r, c-1] == 0 or  #N
                                curr_border == 2 and img[p, r, c+1] == 0 or  #S
                                curr_border == 3 and img[p, r+1, c] == 0 or  #E
                                curr_border == 4 and img[p, r-1, c] == 0 or  #W
                                curr_border == 5 and img[p+1, r, c] == 0 or  #U
                                curr_border == 6 and img[p-1, r, c] == 0)    #B
                if not is_border_pt:
                    # current point is not deletable
                    continue

                get_neighborhood(img, p, r, c, neighborhood)

                # check if (p, r, c) can be deleted:
                # * it must not be an endpoint;
                # * it must be Euler invariant (condition 1 in [Lee94]_); and
                # * it must be simple (i.e., its deletion does not change
                #   connectivity in the 3x3x3 neighborhood)
                #   this is conditions 2 and 3 in [Lee94]_
                if (is_endpoint(neighborhood) or
                    not is_Euler_invariant(neighborhood, Euler_LUT) or
                    not is_simple_point(neighborhood)):
                    continue

                # ok, add (p, r, c) to the list of simple border points
                point.p = p
                point.r = r
                point.c = c
                simple_border_points.push_back(point)


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void get_neighborhood(pixel_type[:, :, ::1] img,
                           npy_intp p, npy_intp r, npy_intp c,
                           pixel_type neighborhood[]) noexcept nogil:
    """Get the neighborhood of a pixel.

    Assume zero boundary conditions.
    Image is already padded, so no out-of-bounds checking.

    For the numbering of points see Fig. 1a. of [Lee94]_, where the numbers
    do *not* include the center point itself. OTOH, this numbering below
    includes it as number 13. The latter is consistent with [IAC15]_.
    """
    neighborhood[0] = img[p-1, r-1, c-1]
    neighborhood[1] = img[p-1, r,   c-1]
    neighborhood[2] = img[p-1, r+1, c-1]

    neighborhood[ 3] = img[p-1, r-1, c]
    neighborhood[ 4] = img[p-1, r,   c]
    neighborhood[ 5] = img[p-1, r+1, c]

    neighborhood[ 6] = img[p-1, r-1, c+1]
    neighborhood[ 7] = img[p-1, r,   c+1]
    neighborhood[ 8] = img[p-1, r+1, c+1]

    neighborhood[ 9] = img[p, r-1, c-1]
    neighborhood[10] = img[p, r,   c-1]
    neighborhood[11] = img[p, r+1, c-1]

    neighborhood[12] = img[p, r-1, c]
    neighborhood[13] = img[p, r,   c]
    neighborhood[14] = img[p, r+1, c]

    neighborhood[15] = img[p, r-1, c+1]
    neighborhood[16] = img[p, r,   c+1]
    neighborhood[17] = img[p, r+1, c+1]

    neighborhood[18] = img[p+1, r-1, c-1]
    neighborhood[19] = img[p+1, r,   c-1]
    neighborhood[20] = img[p+1, r+1, c-1]

    neighborhood[21] = img[p+1, r-1, c]
    neighborhood[22] = img[p+1, r,   c]
    neighborhood[23] = img[p+1, r+1, c]

    neighborhood[24] = img[p+1, r-1, c+1]
    neighborhood[25] = img[p+1, r,   c+1]
    neighborhood[26] = img[p+1, r+1, c+1]


###### look-up tables
def fill_Euler_LUT():
    """ Look-up table for preserving Euler characteristic.

    This is column $\delta G_{26}$ of Table 2 of [Lee94]_.
    """
    cdef int arr[128]
    arr[:] = [1, -1, -1, 1, -3, -1, -1, 1, -1, 1, 1, -1, 3, 1, 1, -1, -3, -1,
                 3, 1, 1, -1, 3, 1, -1, 1, 1, -1, 3, 1, 1, -1, -3, 3, -1, 1, 1,
                 3, -1, 1, -1, 1, 1, -1, 3, 1, 1, -1, 1, 3, 3, 1, 5, 3, 3, 1,
                 -1, 1, 1, -1, 3, 1, 1, -1, -7, -1, -1, 1, -3, -1, -1, 1, -1,
                 1, 1, -1, 3, 1, 1, -1, -3, -1, 3, 1, 1, -1, 3, 1, -1, 1, 1,
                 -1, 3, 1, 1, -1, -3, 3, -1, 1, 1, 3, -1, 1, -1, 1, 1, -1, 3,
                 1, 1, -1, 1, 3, 3, 1, 5, 3, 3, 1, -1, 1, 1, -1, 3, 1, 1, -1]
    cdef ndarray LUT = np.zeros(256, dtype=np.intc)
    LUT[1::2] = arr
    return LUT
cdef int[::1] LUT = fill_Euler_LUT()


# Fill the look-up table for indexing octants for computing the Euler
# characteristic. See is_Euler_invariant routine below.


@cython.boundscheck(False)
@cython.wraparound(False)
cdef bint is_Euler_invariant(pixel_type neighbors[],
                             int[::1] lut) noexcept nogil:
    """Check if a point is Euler invariant.

    Calculate Euler characteristic for each octant and sum up.

    Parameters
    ----------
    neighbors
        neighbors of a point
    lut
        The look-up table for preserving the Euler characteristic.

    Returns
    -------
    bool (C bool, that is)

    """
    cdef int n, euler_char = 0

    # octant 0:
    n = 1
    if neighbors[2] == 1:
        n |= 128

    if neighbors[1] == 1:
        n |= 64

    if neighbors[11] == 1:
        n |= 32

    if neighbors[10] == 1:
        n |= 16

    if neighbors[5] == 1:
        n |= 8

    if neighbors[4] == 1:
        n |= 4

    if neighbors[14] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 1:
    n = 1
    if neighbors[0] == 1:
        n |= 128

    if neighbors[9] == 1:
        n |= 64

    if neighbors[3] == 1:
        n |= 32

    if neighbors[12] == 1:
        n |= 16

    if neighbors[1] == 1:
        n |= 8

    if neighbors[10] == 1:
        n |= 4

    if neighbors[4] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 2:
    n = 1
    if neighbors[8] == 1:
        n |= 128

    if neighbors[7] == 1:
        n |= 64

    if neighbors[17] == 1:
        n |= 32

    if neighbors[16] == 1:
        n |= 16

    if neighbors[5] == 1:
        n |= 8

    if neighbors[4] == 1:
        n |= 4

    if neighbors[14] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 3:
    n = 1
    if neighbors[6] == 1:
        n |= 128

    if neighbors[15] == 1:
        n |= 64

    if neighbors[7] == 1:
        n |= 32

    if neighbors[16] == 1:
        n |= 16

    if neighbors[3] == 1:
        n |= 8

    if neighbors[12] == 1:
        n |= 4

    if neighbors[4] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 4:
    n = 1
    if neighbors[20] == 1:
        n |= 128

    if neighbors[23] == 1:
        n |= 64

    if neighbors[19] == 1:
        n |= 32

    if neighbors[22] == 1:
        n |= 16

    if neighbors[11] == 1:
        n |= 8

    if neighbors[14] == 1:
        n |= 4

    if neighbors[10] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 5:
    n = 1
    if neighbors[18] == 1:
        n |= 128

    if neighbors[21] == 1:
        n |= 64

    if neighbors[9] == 1:
        n |= 32

    if neighbors[12] == 1:
        n |= 16

    if neighbors[19] == 1:
        n |= 8

    if neighbors[22] == 1:
        n |= 4

    if neighbors[10] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 6:
    n = 1
    if neighbors[26] == 1:
        n |= 128

    if neighbors[23] == 1:
        n |= 64

    if neighbors[17] == 1:
        n |= 32

    if neighbors[14] == 1:
        n |= 16

    if neighbors[25] == 1:
        n |= 8

    if neighbors[22] == 1:
        n |= 4

    if neighbors[16] == 1:
        n |= 2

    euler_char += lut[n]

    # octant 7:
    n = 1
    if neighbors[24] == 1:
        n |= 128

    if neighbors[25] == 1:
        n |= 64

    if neighbors[15] == 1:
        n |= 32

    if neighbors[16] == 1:
        n |= 16

    if neighbors[21] == 1:
        n |= 8

    if neighbors[22] == 1:
        n |= 4

    if neighbors[12] == 1:
        n |= 2

    euler_char += lut[n]
    return euler_char == 0


cdef inline bint is_endpoint(pixel_type neighbors[]) noexcept nogil:
    """An endpoint has exactly one neighbor in the 26-neighborhood.
    """
    # The center pixel is counted, thus r.h.s. is 2
    cdef int s = 0, j
    for j in range(27):
        s += neighbors[j]
    return s == 2


cdef bint is_simple_point(pixel_type neighbors[]) noexcept nogil:
    """Check is a point is a Simple Point.

    A point is simple iff its deletion does not change connectivity in
    the 3x3x3 neighborhood. (cf conditions 2 and 3 in [Lee94]_).

    This method is named "N(v)_labeling" in [Lee94]_.

    Parameters
    ----------
    neighbors : uint8 C array, shape(27,)
        neighbors of the point

    Returns
    -------
    bool
        Whether the point is simple or not.

    """
    # copy neighbors for labeling
    # ignore center pixel (i=13) when counting (see [Lee94]_)
    cdef pixel_type cube[26]
    memcpy(cube, neighbors, 13*sizeof(pixel_type))
    memcpy(cube+13, neighbors+14, 13*sizeof(pixel_type))

    # set initial label
    cdef int label = 2, i

    # for all point in the neighborhood
    for i in range(26):
        if cube[i] == 1:
            # voxel has not been labeled yet
            # start recursion with any octant that contains the point i
            if i in (0, 1, 3, 4, 9, 10, 12):
                octree_labeling(1, label, cube)
            elif i in (2, 5, 11, 13):
                octree_labeling(2, label, cube)
            elif i in (6, 7, 14, 15):
                octree_labeling(3, label, cube)
            elif i in (8, 16):
                octree_labeling(4, label, cube)
            elif i in (17, 18, 20, 21):
                octree_labeling(5, label, cube)
            elif i in (19, 22):
                octree_labeling(6, label, cube)
            elif i in (23, 24):
                octree_labeling(7, label, cube)
            elif i == 25:
                octree_labeling(8, label, cube)
            label += 1
            if label - 2 >= 2:
                return False
    return True


# Octree structure for labeling in `octree_labeling` routine below.
# NB: this is only available at build time, and is used by Tempita templating.

@cython.boundscheck(False)
@cython.wraparound(False)
cdef void octree_labeling(int octant, int label, pixel_type cube[]) noexcept nogil:
    """This is a recursive method that calculates the number of connected
    components in the 3D neighborhood after the center pixel would
    have been removed.

    See Figs. 6 and 7 of [Lee94]_ for the values of indices.

    Parameters
    ----------
    octant : int
        octant index
    label : int
        the current label of the center point
    cube : uint8 C array, shape(26,)
        local neighborhood of the point

    """
    # This routine checks if there are points in the octant with value 1
    # Then sets points in this octant to current label
    # and recursive labeling of adjacent octants.
    #
    # Below, leading underscore means build-time variables.

    if octant == 1:
        if cube[0] == 1:
            cube[0] = label
        if cube[1] == 1:
            cube[1] = label
            octree_labeling(2, label, cube)
        if cube[3] == 1:
            cube[3] = label
            octree_labeling(3, label, cube)
        if cube[4] == 1:
            cube[4] = label
            octree_labeling(2, label, cube)
            octree_labeling(3, label, cube)
            octree_labeling(4, label, cube)
        if cube[9] == 1:
            cube[9] = label
            octree_labeling(5, label, cube)
        if cube[10] == 1:
            cube[10] = label
            octree_labeling(2, label, cube)
            octree_labeling(5, label, cube)
            octree_labeling(6, label, cube)
        if cube[12] == 1:
            cube[12] = label
            octree_labeling(3, label, cube)
            octree_labeling(5, label, cube)
            octree_labeling(7, label, cube)

    if octant == 2:
        if cube[1] == 1:
            cube[1] = label
            octree_labeling(1, label, cube)
        if cube[4] == 1:
            cube[4] = label
            octree_labeling(1, label, cube)
            octree_labeling(3, label, cube)
            octree_labeling(4, label, cube)
        if cube[10] == 1:
            cube[10] = label
            octree_labeling(1, label, cube)
            octree_labeling(5, label, cube)
            octree_labeling(6, label, cube)
        if cube[2] == 1:
            cube[2] = label
        if cube[5] == 1:
            cube[5] = label
            octree_labeling(4, label, cube)
        if cube[11] == 1:
            cube[11] = label
            octree_labeling(6, label, cube)
        if cube[13] == 1:
            cube[13] = label
            octree_labeling(4, label, cube)
            octree_labeling(6, label, cube)
            octree_labeling(8, label, cube)

    if octant == 3:
        if cube[3] == 1:
            cube[3] = label
            octree_labeling(1, label, cube)
        if cube[4] == 1:
            cube[4] = label
            octree_labeling(1, label, cube)
            octree_labeling(2, label, cube)
            octree_labeling(4, label, cube)
        if cube[12] == 1:
            cube[12] = label
            octree_labeling(1, label, cube)
            octree_labeling(5, label, cube)
            octree_labeling(7, label, cube)
        if cube[6] == 1:
            cube[6] = label
        if cube[7] == 1:
            cube[7] = label
            octree_labeling(4, label, cube)
        if cube[14] == 1:
            cube[14] = label
            octree_labeling(7, label, cube)
        if cube[15] == 1:
            cube[15] = label
            octree_labeling(4, label, cube)
            octree_labeling(7, label, cube)
            octree_labeling(8, label, cube)

    if octant == 4:
        if cube[4] == 1:
            cube[4] = label
            octree_labeling(1, label, cube)
            octree_labeling(2, label, cube)
            octree_labeling(3, label, cube)
        if cube[5] == 1:
            cube[5] = label
            octree_labeling(2, label, cube)
        if cube[13] == 1:
            cube[13] = label
            octree_labeling(2, label, cube)
            octree_labeling(6, label, cube)
            octree_labeling(8, label, cube)
        if cube[7] == 1:
            cube[7] = label
            octree_labeling(3, label, cube)
        if cube[15] == 1:
            cube[15] = label
            octree_labeling(3, label, cube)
            octree_labeling(7, label, cube)
            octree_labeling(8, label, cube)
        if cube[8] == 1:
            cube[8] = label
        if cube[16] == 1:
            cube[16] = label
            octree_labeling(8, label, cube)

    if octant == 5:
        if cube[9] == 1:
            cube[9] = label
            octree_labeling(1, label, cube)
        if cube[10] == 1:
            cube[10] = label
            octree_labeling(1, label, cube)
            octree_labeling(2, label, cube)
            octree_labeling(6, label, cube)
        if cube[12] == 1:
            cube[12] = label
            octree_labeling(1, label, cube)
            octree_labeling(3, label, cube)
            octree_labeling(7, label, cube)
        if cube[17] == 1:
            cube[17] = label
        if cube[18] == 1:
            cube[18] = label
            octree_labeling(6, label, cube)
        if cube[20] == 1:
            cube[20] = label
            octree_labeling(7, label, cube)
        if cube[21] == 1:
            cube[21] = label
            octree_labeling(6, label, cube)
            octree_labeling(7, label, cube)
            octree_labeling(8, label, cube)

    if octant == 6:
        if cube[10] == 1:
            cube[10] = label
            octree_labeling(1, label, cube)
            octree_labeling(2, label, cube)
            octree_labeling(5, label, cube)
        if cube[11] == 1:
            cube[11] = label
            octree_labeling(2, label, cube)
        if cube[13] == 1:
            cube[13] = label
            octree_labeling(2, label, cube)
            octree_labeling(4, label, cube)
            octree_labeling(8, label, cube)
        if cube[18] == 1:
            cube[18] = label
            octree_labeling(5, label, cube)
        if cube[21] == 1:
            cube[21] = label
            octree_labeling(5, label, cube)
            octree_labeling(7, label, cube)
            octree_labeling(8, label, cube)
        if cube[19] == 1:
            cube[19] = label
        if cube[22] == 1:
            cube[22] = label
            octree_labeling(8, label, cube)

    if octant == 7:
        if cube[12] == 1:
            cube[12] = label
            octree_labeling(1, label, cube)
            octree_labeling(3, label, cube)
            octree_labeling(5, label, cube)
        if cube[14] == 1:
            cube[14] = label
            octree_labeling(3, label, cube)
        if cube[15] == 1:
            cube[15] = label
            octree_labeling(3, label, cube)
            octree_labeling(4, label, cube)
            octree_labeling(8, label, cube)
        if cube[20] == 1:
            cube[20] = label
            octree_labeling(5, label, cube)
        if cube[21] == 1:
            cube[21] = label
            octree_labeling(5, label, cube)
            octree_labeling(6, label, cube)
            octree_labeling(8, label, cube)
        if cube[23] == 1:
            cube[23] = label
        if cube[24] == 1:
            cube[24] = label
            octree_labeling(8, label, cube)

    if octant == 8:
        if cube[13] == 1:
            cube[13] = label
            octree_labeling(2, label, cube)
            octree_labeling(4, label, cube)
            octree_labeling(6, label, cube)
        if cube[15] == 1:
            cube[15] = label
            octree_labeling(3, label, cube)
            octree_labeling(4, label, cube)
            octree_labeling(7, label, cube)
        if cube[16] == 1:
            cube[16] = label
            octree_labeling(4, label, cube)
        if cube[21] == 1:
            cube[21] = label
            octree_labeling(5, label, cube)
            octree_labeling(6, label, cube)
            octree_labeling(7, label, cube)
        if cube[22] == 1:
            cube[22] = label
            octree_labeling(6, label, cube)
        if cube[24] == 1:
            cube[24] = label
            octree_labeling(7, label, cube)
        if cube[25] == 1:
            cube[25] = label

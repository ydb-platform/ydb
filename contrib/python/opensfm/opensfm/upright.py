from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import numpy as np


def opensfm_to_upright(coords, width, height, orientation,
                       new_width=None, new_height=None):
    """
    Transform opensfm coordinates to upright coordinates, correcting for EXIF orientation.

    :param coords: Points in opensfm coordinate system.
    :param width: Width of original image in pixels unadjusted for orientation.
    :param height: Height of original image in pixels unadjusted for orientation.
    :param orientation: Orientation of original image.

    :return: Points in upright coordinate system.

    >>> sfm = np.array([[-0.5, -0.375],
    ...                 [-0.5,  0.375],
    ...                 [ 0.5, -0.375],
    ...                 [ 0.5,  0.375]])
    >>> opensfm_to_upright(sfm, 320, 240, 1)
    array([[   0.,    0.],
           [   0.,  240.],
           [ 320.,    0.],
           [ 320.,  240.]])
    """

    R = {
        1: np.array([[1, 0, 0], [0, 1, 0], [0, 0, 1]]),
        3: np.array([[-1, 0, 1], [0, -1, 1], [0, 0, 1]]),
        6: np.array([[0, -1, 1], [1, 0, 0], [0, 0, 1]]),
        8: np.array([[0, 1, 0], [-1, 0, 1], [0, 0, 1]]),
    }

    w = float(width)
    h = float(height)
    s = max(w, h)

    H = np.array([[s / w,     0, 0.5],
                  [    0, s / h, 0.5],
                  [    0,     0,   1]])

    T = np.dot(R[orientation], H)

    p = np.dot(coords, T[:2, :2].T) + T[:2, 2].reshape(1, 2)

    upright_width = width if orientation < 6 else height
    upright_height = height if orientation < 6 else width
    if new_width is not None:
        upright_width = new_width
    if new_height is not None:
        upright_height = new_height
    p[:, 0] = upright_width * p[:, 0]
    p[:, 1] = upright_height * p[:, 1]

    return p

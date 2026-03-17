# Workaround for issue #378. A pure Python generator.

import numpy as np
from itertools import islice

from rasterio.enums import MaskFlags
from rasterio.windows import Window
from rasterio.transform import rowcol


def _transform_xy(dataset, xy):
    # Transform x, y coordinates to row, col
    # Chunked to reduce calls, thus unnecessary overhead, to rowcol()
    dt = dataset.transform
    _xy = iter(xy)
    while True:
        buf = tuple(islice(_xy, 0, 256))
        if not buf:
            break
        x, y = rowcol(dt, *zip(*buf))
        yield from zip(x,y)

def sort_xy(xy):
    """Sort x, y coordinates by x then y

    Parameters
    ----------
    xy : iterable
        Pairs of x, y coordinates

    Returns
    -------
    list
        A list of sorted x, y coordinates
    """
    x, y = tuple(zip(*xy))
    rv = []
    for ind in np.lexsort([y, x]):
        rv.append((x[ind], y[ind]))
    return rv


def sample_gen(dataset, xy, indexes=None, masked=False):
    """Sample pixels from a dataset

    Parameters
    ----------
    dataset : rasterio Dataset
        Opened in "r" mode.
    xy : iterable
        Pairs of x, y coordinates in the dataset's reference system.

        Note: Sorting coordinates can often yield better performance.
        A sort_xy function is provided in this module for convenience.
    indexes : int or list of int
        Indexes of dataset bands to sample.
    masked : bool, default: False
        Whether to mask samples that fall outside the extent of the
        dataset.

    Yields
    ------
    array
        A array of length equal to the number of specified indexes
        containing the dataset values for the bands corresponding to
        those indexes.

    """
    read = dataset.read
    height = dataset.height
    width = dataset.width

    if indexes is None:
        indexes = dataset.indexes
    elif isinstance(indexes, int):
        indexes = [indexes]

    nodata = np.full(len(indexes), (dataset.nodata or 0),  dtype=dataset.dtypes[0])
    if masked:
        # Masks for masked arrays are inverted (False means valid)
        mask_flags = [set(dataset.mask_flag_enums[i - 1]) for i in indexes]
        dataset_is_masked = any(
            {MaskFlags.alpha, MaskFlags.per_dataset, MaskFlags.nodata} & enums
            for enums in mask_flags
        )
        mask = [
            False if dataset_is_masked and enums == {MaskFlags.all_valid} else True
            for enums in mask_flags
        ]
        nodata = np.ma.array(nodata, mask=mask)

    for row, col in _transform_xy(dataset, xy):
        if 0 <= row < height and 0 <= col < width:
            win = Window(col, row, 1, 1)
            data = read(indexes, window=win, masked=masked)
            yield data[:, 0, 0]
        else:
            yield nodata

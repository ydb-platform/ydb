"""
Common functions for reshaping numpy arrays
"""

from typing import Hashable, MutableMapping

import numpy as np
import stanio


def flatten_chains(draws_array: np.ndarray) -> np.ndarray:
    """
    Flatten a 3D array of draws X chains X variable into 2D array
    where all chains are concatenated into a single column.

    :param draws_array: 3D array of draws
    """
    if len(draws_array.shape) != 3:
        raise ValueError(
            'Expecting 3D array, found array with {} dims'.format(
                len(draws_array.shape)
            )
        )

    num_rows = draws_array.shape[0] * draws_array.shape[1]
    num_cols = draws_array.shape[2]
    return draws_array.reshape((num_rows, num_cols), order='F')


def build_xarray_data(
    data: MutableMapping[Hashable, tuple[tuple[str, ...], np.ndarray]],
    var: stanio.Variable,
    drawset: np.ndarray,
) -> None:
    """
    Adds Stan variable name, labels, and values to a dictionary
    that will be used to construct an xarray DataSet.
    """
    var_dims: tuple[str, ...] = ('draw', 'chain')
    var_dims += tuple(f"{var.name}_dim_{i}" for i in range(len(var.dimensions)))

    data[var.name] = (
        var_dims,
        var.extract_reshape(drawset),
    )

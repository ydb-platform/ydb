"""
Module to load the minimal information from a Stan CSV file.
Only the header row and data are read, no metadata is parsed.
"""
from typing import List, Tuple, Union

import numpy as np
import numpy.typing as npt


def read_csv(filenames: Union[str, List[str]]) -> Tuple[str, npt.NDArray[np.float64]]:
    """
    Reads CSV files like those produced by Stan, returning the header and data.

    If multiple files are given, the data is stacked along the first axis,
    so in typical usage, the shape of the returned data will be
    ``(n_chains, n_samples, n_params)``.

    Parameters
    ----------
    filenames : Union[str, List[str]]
        Path to the CSV file(s) to read.

    Returns
    -------
    Tuple[str, npt.NDArray[np.float64]]
        The header row and data from the CSV file(s).

    Raises
    ------
    ValueError
        If multiple files are given and the headers do not match between them.
    """

    if not isinstance(filenames, list):
        filenames = [filenames]

    header = ""
    data: List[npt.NDArray[np.float64]] = [None for _ in range(len(filenames))]  # type: ignore
    for i, f in enumerate(filenames):
        with open(f, "r") as fd:
            while (file_header := fd.readline()).startswith("#"):
                pass
            if header == "":
                header = file_header
            elif header != file_header:
                raise ValueError("Headers do not match")
            data[i] = np.loadtxt(fd, delimiter=",", comments="#")

    return header.strip(), np.stack(data, axis=0)

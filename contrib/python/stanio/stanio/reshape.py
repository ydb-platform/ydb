"""
Classes and functions for reshaping Stan output.

Especially with the addition of tuples, Stan writes
flat arrays of data with a rich internal structure.
"""
from dataclasses import dataclass
from enum import Enum
from math import prod
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import numpy.typing as npt


class VariableType(Enum):
    SCALAR = 1  # real or integer
    COMPLEX = 2  # complex number - requires striding
    TUPLE = 3  # tuples - require recursive handling


@dataclass
class Variable:
    """
    This class represents a single output variable of a Stan model.

    It contains information about the name, dimensions, and type of the
    variable, as well as the indices of where that variable is located in
    the flattened output array Stan models write.

    Generally, this class should not be instantiated directly, but rather
    created by the :func:`parse_header()` function.
    """

    # name of the parameter as given in stan. For nested parameters, this is a dummy name
    name: str
    # where to start (resp. end) reading from the flattened array.
    # For arrays with nested parameters, this will be for the first element
    # and is relative to the start of the parent
    start_idx: int
    end_idx: int
    # rectangular dimensions of the parameter (e.g. (2, 3) for a 2x3 matrix)
    # For nested parameters, this will be the dimensions of the outermost array.
    dimensions: Tuple[int, ...]
    # type of the parameter
    type: VariableType
    # list of nested parameters
    contents: List["Variable"]

    def dtype(self, top: bool = True) -> np.dtype:
        if self.type == VariableType.TUPLE:
            elts = [
                (str(i + 1), param.dtype(top=False))
                for i, param in enumerate(self.contents)
            ]
            dtype = np.dtype(elts)
        elif self.type == VariableType.SCALAR:
            dtype = np.float64
        elif self.type == VariableType.COMPLEX:
            dtype = np.complex128

        if top:
            return dtype
        else:
            return np.dtype((dtype, self.dimensions))

    def columns(self) -> Iterable[int]:
        return range(self.start_idx, self.end_idx)

    def num_elts(self) -> int:
        return prod(self.dimensions)

    def elt_size(self) -> int:
        return self.end_idx - self.start_idx

    # total size is elt_size * num_elts

    def _extract_helper(self, src: np.ndarray, offset: int = 0) -> np.ndarray:
        start = self.start_idx + offset
        end = self.end_idx + offset
        if self.type == VariableType.SCALAR:
            return src[..., start:end].reshape(-1, *self.dimensions, order="F")
        elif self.type == VariableType.COMPLEX:
            ret = src[..., start:end].reshape(-1, 2, *self.dimensions, order="F")
            ret = ret[:, ::2] + 1j * ret[:, 1::2]
            return ret.squeeze().reshape(-1, *self.dimensions, order="F")
        elif self.type == VariableType.TUPLE:
            out: np.ndarray = np.empty(
                (prod(src.shape[:-1]), prod(self.dimensions)), dtype=object
            )
            for idx in range(self.num_elts()):
                off = idx * self.elt_size() // self.num_elts()
                elts = [
                    param._extract_helper(src, offset=start + off)
                    for param in self.contents
                ]
                for i in range(elts[0].shape[0]):
                    out[i, idx] = tuple(elt[i] for elt in elts)
            return out.reshape(-1, *self.dimensions, order="F")

    def extract_reshape(self, src: np.ndarray, object: bool = True) -> npt.NDArray[Any]:
        """
        Given an array where the final dimension is the flattened output of a
        Stan model, (e.g. one row of a Stan CSV file), extract the variable
        and reshape it to the correct type and dimensions.

        This will most likely result in copies of the data being made if
        the variable is not a scalar.

        Parameters
        ----------
        src : np.ndarray
            The array to extract from.

            Indicies besides the final dimension are preserved
            in the output.

        object : bool
            If True, the output of tuple types will be an object array,
            otherwise it will use custom dtypes to represent tuples.

        Returns
        -------
        npt.NDArray[Any]
            The extracted variable, reshaped to the correct dimensions.
            If the variable is a tuple, this will be an object array,
            otherwise it will have a dtype of either float64 or complex128.
        """
        out = self._extract_helper(src)
        if not object:
            out = out.astype(self.dtype())
        if src.ndim > 1:
            out = out.reshape(*src.shape[:-1], *self.dimensions, order="F")
        else:
            out = out.squeeze(axis=0)

        return out


def _munge_first_tuple(tup: str) -> str:
    return "dummy_" + tup.split(":", 1)[1]


def _get_base_name(param: str) -> str:
    return param.split(".")[0].split(":")[0]


def _from_header(header: str) -> List[Variable]:
    # appending __dummy ensures one extra iteration in the later loop
    header = header.strip() + ",__dummy"
    entries = header.split(",")
    params = []
    start_idx = 0
    name = _get_base_name(entries[0])
    for i in range(0, len(entries) - 1):
        entry = entries[i]
        next_name = _get_base_name(entries[i + 1])

        if next_name != name:
            if ":" not in entry:
                dims = entry.split(".")[1:]
                if ".real" in entry or ".imag" in entry:
                    type = VariableType.COMPLEX
                    dims = dims[:-1]
                else:
                    type = VariableType.SCALAR
                params.append(
                    Variable(
                        name=name,
                        start_idx=start_idx,
                        end_idx=i + 1,
                        dimensions=tuple(map(int, dims)),
                        type=type,
                        contents=[],
                    )
                )
            else:
                dims = entry.split(":")[0].split(".")[1:]
                munged_header = ",".join(
                    dict.fromkeys(map(_munge_first_tuple, entries[start_idx : i + 1]))
                )

                params.append(
                    Variable(
                        name=name,
                        start_idx=start_idx,
                        end_idx=i + 1,
                        dimensions=tuple(map(int, dims)),
                        type=VariableType.TUPLE,
                        contents=_from_header(munged_header),
                    )
                )

            start_idx = i + 1
            name = next_name

    return params


def parse_header(header: str) -> Dict[str, Variable]:
    """
    Given a comma-separated list of names of Stan outputs, like
    that from the header row of a CSV file, parse it into a dictionary of
    :class:`Variable` objects.

    Parameters
    ----------
    header : str
        Comma separated list of Stan variables, including index information.
        For example, an ``array[2] real foo` would be represented as
        ``foo.1,foo.2``.

    Returns
    -------
    Dict[str, Variable]
        A dictionary mapping the base name of each variable to a :class:`Variable`.
    """
    return {param.name: param for param in _from_header(header)}


def stan_variables(
    parameters: Dict[str, Variable],
    source: npt.NDArray[np.float64],
    *,
    object: bool = True,
) -> Dict[str, npt.NDArray[Any]]:
    """
    Given a dictionary of :class:`Variable` objects and a source array,
    extract the variables from the source array and reshape them to the
    correct dimensions.

    Parameters
    ----------
    parameters : Dict[str, Variable]
        A dictionary of :class:`Variable` objects,
        like that returned by :func:`parse_header()`.
    source : npt.NDArray[np.float64]
        The array to extract from.
    object : bool
        If True, the output of tuple types will be an object array,
        otherwise it will use custom dtypes to represent tuples.

    Returns
    -------
    Dict[str, npt.NDArray[Any]]
        A dictionary mapping the base name of each variable to the extracted
        and reshaped data.
    """
    return {
        param.name: param.extract_reshape(source, object=object)
        for param in parameters.values()
    }

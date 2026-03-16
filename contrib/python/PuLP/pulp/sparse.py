from typing import Dict, Generic, List, Tuple, TypeVar, cast

# Sparse : Python basic dictionary sparse matrix

# Copyright (c) 2007, Stuart Mitchell (s.mitchell@auckland.ac.nz)
# $Id: sparse.py 1704 2007-12-20 21:56:14Z smit023 $

# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
sparse this module provides basic pure python sparse matrix implementation
notably this allows the sparse matrix to be output in various formats
"""

T = TypeVar("T")


class Matrix(Generic[T], Dict[Tuple[int, int], T]):
    """This is a dictionary based sparse matrix class"""

    def __init__(self, rows: List[int], cols: List[int]):
        """initialises the class by creating a matrix that will have the given
        rows and columns
        """
        self.rows: List[int] = rows
        self.cols: List[int] = cols
        self.rowdict: Dict[int, Dict[int, T]] = {row: {} for row in rows}
        self.coldict: Dict[int, Dict[int, T]] = {col: {} for col in cols}

    def add(
        self,
        row: int,
        col: int,
        item: T,
        colcheck: bool = False,
        rowcheck: bool = False,
    ) -> None:
        if not rowcheck or row in self.rows:
            if not colcheck or col in self.cols:
                dict.__setitem__(self, (row, col), item)
                self.rowdict[row][col] = item
                self.coldict[col][row] = item
            else:
                print(self.cols)
                raise RuntimeError(f"col {col} is not in the matrix columns")
        else:
            raise RuntimeError(f"row {row} is not in the matrix rows")

    def addcol(self, col: int, rowitems: Dict[int, T]) -> None:
        """adds a column"""
        if col in self.cols:
            for row, item in rowitems.items():
                self.add(row, col, item, colcheck=False)
        else:
            raise RuntimeError("col is not in the matrix columns")

    def get(  # type: ignore[override]
        self,
        coords: Tuple[int, int],
        default: T = 0,  # type: ignore[assignment]
    ) -> T:
        return dict.get(self, coords, default)

    def col_based_arrays(
        self,
    ) -> Tuple[int, List[int], List[int], List[int], List[T]]:
        numEls = len(self)
        elemBase: List[T] = []
        startsBase = []
        indBase = []
        lenBase = []
        for i, col in enumerate(self.cols):
            startsBase.append(len(elemBase))
            elemBase.extend(list(self.coldict[col].values()))
            indBase.extend(list(self.coldict[col].keys()))
            lenBase.append(len(elemBase) - startsBase[-1])

        startsBase.append(len(elemBase))
        return numEls, startsBase, lenBase, indBase, elemBase

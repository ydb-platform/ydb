# Copyright (c) 2018-2024, Manfred Moitzi
# License: MIT License
# legacy.py code is replaced by numpy - exists just for benchmarking, testing and nostalgia
from __future__ import annotations
from typing import Iterable, cast
from itertools import repeat
import reprlib

from .linalg import Matrix, MatrixData, NDArray, Solver

__all__ = [
    "gauss_vector_solver",
    "gauss_matrix_solver",
    "gauss_jordan_solver",
    "gauss_jordan_inverse",
    "LUDecomposition",
]


def copy_float_matrix(A) -> MatrixData:
    if isinstance(A, Matrix):
        A = A.matrix
    return [[float(v) for v in row] for row in A]


def gauss_vector_solver(A: MatrixData | NDArray, B: Iterable[float]) -> list[float]:
    """Solves the linear equation system given by a nxn Matrix A . x = B,
    right-hand side quantities as vector B with n elements by the
    `Gauss-Elimination`_ algorithm, which is faster than the `Gauss-Jordan`_
    algorithm. The speed improvement is more significant for solving multiple
    right-hand side quantities as matrix at once.

    Reference implementation for error checking.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], [a21, a22, ..., a2n],
            ... [an1, an2, ..., ann]]
        B: vector [b1, b2, ..., bn]

    Returns:
        vector as list of floats

    Raises:
        ZeroDivisionError: singular matrix

    """
    # copy input data
    A = copy_float_matrix(A)
    B = list(B)
    num = len(A)
    if len(A[0]) != num:
        raise ValueError("A square nxn matrix A is required.")
    if len(B) != num:
        raise ValueError(
            "Item count of vector B has to be equal to matrix A row count."
        )

    # inplace modification of A & B
    _build_upper_triangle(A, B)
    return _backsubstitution(A, B)


def gauss_matrix_solver(A: MatrixData | NDArray, B: MatrixData | NDArray) -> Matrix:
    """Solves the linear equation system given by a nxn Matrix A . x = B,
    right-hand side quantities as nxm Matrix B by the `Gauss-Elimination`_
    algorithm, which is faster than the `Gauss-Jordan`_ algorithm.

    Reference implementation for error checking.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], [a21, a22, ..., a2n],
            ... [an1, an2, ..., ann]]
        B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m], ... [bn1, bn2, ..., bnm]]

    Returns:
        matrix as :class:`Matrix` object

    Raises:
        ZeroDivisionError: singular matrix

    """
    # copy input data
    matrix_a = copy_float_matrix(A)
    matrix_b = copy_float_matrix(B)
    num = len(matrix_a)
    if len(matrix_a[0]) != num:
        raise ValueError("A square nxn matrix A is required.")
    if len(matrix_b) != num:
        raise ValueError("Row count of matrices A and B has to match.")

    # inplace modification of A & B
    _build_upper_triangle(matrix_a, matrix_b)

    columns = Matrix(matrix=matrix_b).cols()
    result = Matrix()
    for col in columns:
        result.append_col(_backsubstitution(matrix_a, col))

    return result


def _build_upper_triangle(A: MatrixData, B: list) -> None:
    """Build upper triangle for backsubstitution. Modifies A and B inplace!

    Args:
         A: row major matrix
         B: vector of floats or row major matrix

    """
    num = len(A)
    try:
        b_col_count = len(B[0])
    except TypeError:
        b_col_count = 1

    for i in range(0, num):
        # Search for maximum in this column
        max_element = abs(A[i][i])
        max_row = i
        for row in range(i + 1, num):
            value = abs(A[row][i])
            if value > max_element:
                max_element = value
                max_row = row

        # Swap maximum row with current row
        A[max_row], A[i] = A[i], A[max_row]
        B[max_row], B[i] = B[i], B[max_row]

        # Make all rows below this one 0 in current column
        for row in range(i + 1, num):
            c = -A[row][i] / A[i][i]
            for col in range(i, num):
                if i == col:
                    A[row][col] = 0
                else:
                    A[row][col] += c * A[i][col]
            if b_col_count == 1:
                B[row] += c * B[i]
            else:
                for col in range(b_col_count):
                    B[row][col] += c * B[i][col]


def _backsubstitution(A: MatrixData, B: list[float]) -> list[float]:
    """Solve equation A . x = B for an upper triangular matrix A by
    backsubstitution.

    Args:
        A: row major matrix
        B: vector of floats

    """
    num = len(A)
    x = [0.0] * num
    for i in range(num - 1, -1, -1):
        x[i] = B[i] / A[i][i]
        for row in range(i - 1, -1, -1):
            B[row] -= A[row][i] * x[i]
    return x


def gauss_jordan_solver(
    A: MatrixData | NDArray, B: MatrixData | NDArray
) -> tuple[Matrix, Matrix]:
    """Solves the linear equation system given by a nxn Matrix A . x = B,
    right-hand side quantities as nxm Matrix B by the `Gauss-Jordan`_ algorithm,
    which is the slowest of all, but it is very reliable. Returns a copy of the
    modified input matrix `A` and the result matrix `x`.

    Internally used for matrix inverse calculation.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], [a21, a22, ..., a2n],
            ... [an1, an2, ..., ann]]
        B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m], ... [bn1, bn2, ..., bnm]]

    Returns:
        2-tuple of :class:`Matrix` objects

    Raises:
        ZeroDivisionError: singular matrix

    """
    # copy input data
    matrix_a = copy_float_matrix(A)
    matrix_b = copy_float_matrix(B)

    n = len(matrix_a)
    m = len(matrix_b[0])

    if len(matrix_a[0]) != n:
        raise ValueError("A square nxn matrix A is required.")
    if len(matrix_b) != n:
        raise ValueError("Row count of matrices A and B has to match.")

    icol = 0
    irow = 0
    col_indices = [0] * n
    row_indices = [0] * n
    ipiv = [0] * n

    for i in range(n):
        big = 0.0
        for j in range(n):
            if ipiv[j] != 1:
                for k in range(n):
                    if ipiv[k] == 0:
                        if abs(matrix_a[j][k]) >= big:
                            big = abs(matrix_a[j][k])
                            irow = j
                            icol = k

        ipiv[icol] += 1
        if irow != icol:
            matrix_a[irow], matrix_a[icol] = matrix_a[icol], matrix_a[irow]
            matrix_b[irow], matrix_b[icol] = matrix_b[icol], matrix_b[irow]

        row_indices[i] = irow
        col_indices[i] = icol

        pivinv = 1.0 / matrix_a[icol][icol]
        matrix_a[icol][icol] = 1.0
        matrix_a[icol] = [v * pivinv for v in matrix_a[icol]]
        matrix_b[icol] = [v * pivinv for v in matrix_b[icol]]
        for row in range(n):
            if row == icol:
                continue
            dum = matrix_a[row][icol]
            matrix_a[row][icol] = 0.0
            for col in range(n):
                matrix_a[row][col] -= matrix_a[icol][col] * dum
            for col in range(m):
                matrix_b[row][col] -= matrix_b[icol][col] * dum

    for i in range(n - 1, -1, -1):
        irow = row_indices[i]
        icol = col_indices[i]
        if irow != icol:
            for _row in matrix_a:
                _row[irow], _row[icol] = _row[icol], _row[irow]
    return Matrix(matrix=matrix_a), Matrix(matrix=matrix_b)


def gauss_jordan_inverse(A: MatrixData) -> Matrix:
    """Returns the inverse of matrix `A` as :class:`Matrix` object.

    .. hint::

        For small matrices (n<10) is this function faster than
        LUDecomposition(m).inverse() and as fast even if the decomposition is
        already done.

    Raises:
        ZeroDivisionError: singular matrix

    """
    if isinstance(A, Matrix):
        matrix_a = A.matrix
    else:
        matrix_a = list(A)
    nrows = len(matrix_a)
    return gauss_jordan_solver(matrix_a, list(repeat([0.0], nrows)))[0]


class LUDecomposition(Solver):
    """Represents a `LU decomposition`_ matrix of A, raise :class:`ZeroDivisionError`
    for a singular matrix.

    This algorithm is a little bit faster than the `Gauss-Elimination`_
    algorithm using CPython and much faster when using pypy.

    The :attr:`LUDecomposition.matrix` attribute gives access to the matrix data
    as list of rows like in the :class:`Matrix` class, and the :attr:`LUDecomposition.index`
    attribute gives access to the swapped row indices.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], [a21, a22, ..., a2n],
            ... [an1, an2, ..., ann]]

    raises:
        ZeroDivisionError: singular matrix

    """

    __slots__ = ("matrix", "index", "_det")

    def __init__(self, A: MatrixData | NDArray):
        lu: MatrixData = copy_float_matrix(A)
        n: int = len(lu)
        det: float = 1.0
        index: list[int] = []

        # find max value for each row, raises ZeroDivisionError for singular matrix!
        scaling: list[float] = [1.0 / max(abs(v) for v in row) for row in lu]

        for k in range(n):
            big: float = 0.0
            imax: int = k
            for i in range(k, n):
                temp: float = scaling[i] * abs(lu[i][k])
                if temp > big:
                    big = temp
                    imax = i

            if k != imax:
                for col in range(n):
                    temp = lu[imax][col]
                    lu[imax][col] = lu[k][col]
                    lu[k][col] = temp

                det = -det
                scaling[imax] = scaling[k]

            index.append(imax)
            for i in range(k + 1, n):
                temp = lu[i][k] / lu[k][k]
                lu[i][k] = temp
                for j in range(k + 1, n):
                    lu[i][j] -= temp * lu[k][j]

        self.index: list[int] = index
        self.matrix: MatrixData = lu
        self._det: float = det

    def __str__(self) -> str:
        return str(self.matrix)

    def __repr__(self) -> str:
        return f"{self.__class__} {reprlib.repr(self.matrix)}"

    @property
    def nrows(self) -> int:
        """Count of matrix rows (and cols)."""
        return len(self.matrix)

    def solve_vector(self, B: Iterable[float]) -> list[float]:
        """Solves the linear equation system given by the nxn Matrix A . x = B,
        right-hand side quantities as vector B with n elements.

        Args:
            B: vector [b1, b2, ..., bn]

        Returns:
            vector as list of floats

        """
        X: list[float] = [float(v) for v in B]
        lu: MatrixData = self.matrix
        index: list[int] = self.index
        n: int = self.nrows
        ii: int = 0

        if len(X) != n:
            raise ValueError(
                "Item count of vector B has to be equal to matrix row count."
            )

        for i in range(n):
            ip: int = index[i]
            sum_: float = X[ip]
            X[ip] = X[i]
            if ii != 0:
                for j in range(ii - 1, i):
                    sum_ -= lu[i][j] * X[j]
            elif sum_ != 0.0:
                ii = i + 1
            X[i] = sum_

        for row in range(n - 1, -1, -1):
            sum_ = X[row]
            for col in range(row + 1, n):
                sum_ -= lu[row][col] * X[col]
            X[row] = sum_ / lu[row][row]
        return X

    def solve_matrix(self, B: MatrixData | NDArray) -> Matrix:
        """Solves the linear equation system given by the nxn Matrix A . x = B,
        right-hand side quantities as nxm Matrix B.

        Args:
            B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m],
                ... [bn1, bn2, ..., bnm]]

        Returns:
            matrix as :class:`Matrix` object

        """
        if not isinstance(B, Matrix):
            matrix_b = Matrix(matrix=[list(row) for row in B])
        else:
            matrix_b = cast(Matrix, B)

        if matrix_b.nrows != self.nrows:
            raise ValueError("Row count of self and matrix B has to match.")

        return Matrix(
            matrix=[self.solve_vector(col) for col in matrix_b.cols()]
        ).transpose()

    def inverse(self) -> Matrix:
        """Returns the inverse of matrix as :class:`Matrix` object,
        raise :class:`ZeroDivisionError` for a singular matrix.

        """
        return self.solve_matrix(Matrix.identity(shape=(self.nrows, self.nrows)).matrix)

    def determinant(self) -> float:
        """Returns the determinant of matrix, raises :class:`ZeroDivisionError`
        if matrix is singular.

        """
        det: float = self._det
        lu: MatrixData = self.matrix
        for i in range(self.nrows):
            det *= lu[i][i]
        return det

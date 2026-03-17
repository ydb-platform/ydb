# Copyright (c) 2018-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Tuple,
    List,
    Sequence,
    Any,
    cast,
    Optional,
)
from typing_extensions import TypeAlias
import abc

from functools import lru_cache
from itertools import repeat
import math
import reprlib

import numpy as np
import numpy.typing as npt
from ezdxf.acc import USE_C_EXT


__all__ = [
    "Matrix",
    "Solver",
    "numpy_vector_solver",
    "numpy_matrix_solver",
    "NumpySolver",
    "tridiagonal_vector_solver",
    "tridiagonal_matrix_solver",
    "detect_banded_matrix",
    "compact_banded_matrix",
    "BandedMatrixLU",
    "banded_matrix",
    "quadratic_equation",
    "cubic_equation",
    "binomial_coefficient",
]


def zip_to_list(*args) -> Iterable[list]:
    for e in zip(*args):  # returns immutable tuples
        yield list(e)  # need mutable list


MatrixData: TypeAlias = List[List[float]]
FrozenMatrixData: TypeAlias = Tuple[Tuple[float, ...]]
Shape: TypeAlias = Tuple[int, int]
NDArray: TypeAlias = npt.NDArray[np.float64]


def copy_float_matrix(A) -> MatrixData:
    if isinstance(A, Matrix):
        A = A.matrix
    return [[float(v) for v in row] for row in A]


@lru_cache(maxsize=128)
def binomial_coefficient(k: int, i: int) -> float:
    # (c) Onur Rauf Bingol <orbingol@gmail.com>, NURBS-Python, MIT-License
    """Computes the binomial coefficient (denoted by `k choose i`).

    Please see the following website for details:
    http://mathworld.wolfram.com/BinomialCoefficient.html

    Args:
        k: size of the set of distinct elements
        i: size of the subsets

    """
    # Special case
    if i > k:
        return float(0)
    # Compute binomial coefficient
    k_fact: int = math.factorial(k)
    i_fact: int = math.factorial(i)
    k_i_fact: int = math.factorial(k - i)
    return float(k_fact / (k_i_fact * i_fact))


class Matrix:
    """Basic matrix implementation based :class:`numpy.ndarray`. Matrix data is stored in
    row major order, this means in a list of rows, where each row is a list of floats.

    Initialization:

        - Matrix(shape=(rows, cols)) ... new matrix filled with zeros
        - Matrix(matrix[, shape=(rows, cols)]) ... from copy of matrix and optional reshape
        - Matrix([[row_0], [row_1], ..., [row_n]]) ... from Iterable[Iterable[float]]
        - Matrix([a1, a2, ..., an], shape=(rows, cols)) ... from Iterable[float] and shape

    .. versionchanged:: 1.2
        Implementation based on :class:`numpy.ndarray`.

    Attributes:
        matrix: matrix data as :class:`numpy.ndarray`

    """

    __slots__ = ("matrix", "abs_tol")

    def __init__(
        self,
        items: Any = None,
        shape: Optional[Shape] = None,
        matrix: Optional[MatrixData | NDArray] = None,
    ):
        self.abs_tol: float = 1e-12
        self.matrix: NDArray = np.array((), dtype=np.float64)
        if matrix is not None:
            self.matrix = np.array(matrix, dtype=np.float64)
            return

        if items is None:
            if shape is not None:
                self.matrix = np.zeros(shape)
            else:  # items is None, shape is None
                return
        elif isinstance(items, Matrix):
            if shape is None:
                shape = items.shape
            self.matrix = items.matrix.reshape(shape).copy()
        else:
            items = list(items)
            try:
                self.matrix = np.array([list(row) for row in items], dtype=np.float64)
            except TypeError:
                if shape is not None:
                    self.matrix = np.array(list(items), dtype=np.float64).reshape(shape)

    def __iter__(self) -> NDArray:
        return np.ravel(self.matrix)

    def __copy__(self) -> "Matrix":
        m = Matrix(matrix=self.matrix.copy())
        m.abs_tol = self.abs_tol
        return m

    def __str__(self) -> str:
        return str(self.matrix)

    def __repr__(self) -> str:
        return f"Matrix({reprlib.repr(self.matrix)})"

    @staticmethod
    def reshape(items: Iterable[float], shape: Shape) -> Matrix:
        """Returns a new matrix for iterable `items` in the configuration of
        `shape`.
        """
        return Matrix(matrix=np.array(list(items), dtype=np.float64).reshape(shape))

    @property
    def nrows(self) -> int:
        """Count of matrix rows."""
        return self.matrix.shape[0]

    @property
    def ncols(self) -> int:
        """Count of matrix columns."""
        return self.matrix.shape[1]

    @property
    def shape(self) -> Shape:
        """Shape of matrix as (n, m) tuple for n rows and m columns."""
        return self.matrix.shape  # type: ignore

    def row(self, index: int) -> list[float]:
        """Returns row `index` as list of floats."""
        return list(self.matrix[index])

    def col(self, index: int) -> list[float]:
        """Return column `index` as list of floats."""
        return list(self.matrix[:, index])

    def diag(self, index: int) -> list[float]:
        """Returns diagonal `index` as list of floats.

        An `index` of 0 specifies the main diagonal, negative values
        specifies diagonals below the main diagonal and positive values
        specifies diagonals above the main diagonal.

        e.g. given a 4x4 matrix:

        - index 0 is [00, 11, 22, 33],
        - index -1 is [10, 21, 32] and
        - index +1 is [01, 12, 23]

        """
        return list(self.matrix.diagonal(index))

    def rows(self) -> list[list[float]]:
        """Return a list of all rows."""
        return list(list(r) for r in self.matrix)

    def cols(self) -> list[list[float]]:
        """Return a list of all columns."""
        return [list(self.col(i)) for i in range(self.ncols)]

    def set_row(self, index: int, items: float | Iterable[float] = 1.0) -> None:
        """Set row values to a fixed value or from an iterable of floats."""
        if isinstance(items, (float, int)):
            items = [float(items)] * self.ncols
        items = list(items)
        if len(items) != self.ncols:
            raise ValueError("Invalid item count")
        self.matrix[index] = items

    def set_col(self, index: int, items: float | Iterable[float] = 1.0) -> None:
        """Set column values to a fixed value or from an iterable of floats."""
        if isinstance(items, (float, int)):
            items = [float(items)] * self.nrows
        self.matrix[:, index] = list(items)

    def set_diag(self, index: int = 0, items: float | Iterable[float] = 1.0) -> None:
        """Set diagonal values to a fixed value or from an iterable of floats.

        An `index` of ``0`` specifies the main diagonal, negative values
        specifies diagonals below the main diagonal and positive values
        specifies diagonals above the main diagonal.

        e.g. given a 4x4 matrix:
        index ``0`` is [00, 11, 22, 33],
        index ``-1`` is [10, 21, 32] and
        index ``+1`` is [01, 12, 23]

        """
        if isinstance(items, (float, int)):
            items = repeat(float(items))

        col_offset: int = max(index, 0)
        row_offset: int = abs(min(index, 0))

        for index, value in zip(range(max(self.nrows, self.ncols)), items):
            try:
                self.matrix[index + row_offset, index + col_offset] = value
            except IndexError:
                return

    @classmethod
    def identity(cls, shape: Shape) -> Matrix:
        """Returns the identity matrix for configuration `shape`."""
        m = Matrix(shape=shape)
        m.set_diag(0, 1.0)
        return m

    def append_row(self, items: Sequence[float]) -> None:
        """Append a row to the matrix."""
        if self.matrix.size == 0:
            self.matrix = np.array([items], dtype=np.float64)
        elif len(items) == self.ncols:
            self.matrix = np.r_[self.matrix, items]
        else:
            raise ValueError("Invalid item count.")

    def append_col(self, items: Sequence[float]) -> None:
        """Append a column to the matrix."""
        if self.matrix.size == 0:
            self.matrix = np.array([[item] for item in items], dtype=np.float64)
        elif len(items) == self.nrows:
            self.matrix = np.c_[self.matrix, items]
        else:
            raise ValueError("Invalid item count.")

    def freeze(self) -> Matrix:
        """Returns a frozen matrix, all data is stored in immutable tuples."""
        m = self.__copy__()
        m.matrix.flags.writeable = False
        return m

    def __getitem__(self, item: tuple[int, int]) -> float:
        """Get value by (row, col) index tuple, fancy slicing as known from
        numpy is not supported.

        """
        return float(self.matrix[item])

    def __setitem__(self, item: tuple[int, int], value: float):
        """Set value by (row, col) index tuple, fancy slicing as known from
        numpy is not supported.

        """
        self.matrix[item] = value

    def __eq__(self, other: object) -> bool:
        """Returns ``True`` if matrices are equal."""
        if not isinstance(other, Matrix):
            raise TypeError("Matrix class required.")
        if self.shape != other.shape:
            raise TypeError("Matrices have different shapes.")
        return bool(np.all(self.matrix == other.matrix))

    def isclose(self, other: object) -> bool:
        """Returns ``True`` if matrices are close to equal, tolerance value for
        comparison is adjustable by the attribute :attr:`Matrix.abs_tol`.

        """
        if not isinstance(other, Matrix):
            raise TypeError("Matrix class required.")
        if self.shape != other.shape:
            raise TypeError("Matrices have different shapes.")
        return bool(np.all(np.isclose(self.matrix, other.matrix, atol=self.abs_tol)))

    def __mul__(self, other: Matrix | float) -> Matrix:
        """Matrix multiplication by another matrix or a float, returns a new
        matrix.

        """
        if isinstance(other, Matrix):
            return Matrix(matrix=np.matmul(self.matrix, other.matrix))
        else:
            matrix = Matrix(matrix=self.matrix * float(other))  # type: ignore
        return matrix

    __imul__ = __mul__

    def __add__(self, other: Matrix | float) -> Matrix:
        """Matrix addition by another matrix or a float, returns a new matrix."""
        if isinstance(other, Matrix):
            return Matrix(matrix=self.matrix + other.matrix)  # type: ignore
        else:
            return Matrix(matrix=self.matrix + float(other))  # type: ignore

    __iadd__ = __add__

    def __sub__(self, other: Matrix | float) -> Matrix:
        """Matrix subtraction by another matrix or a float, returns a new
        matrix.

        """
        if isinstance(other, Matrix):
            return Matrix(matrix=self.matrix - other.matrix)  # type: ignore
        else:
            return Matrix(matrix=self.matrix - float(other))  # type: ignore

    __isub__ = __sub__

    def transpose(self) -> Matrix:
        """Returns a new transposed matrix."""
        return Matrix(matrix=self.matrix.T)

    def inverse(self) -> Matrix:
        """Returns inverse of matrix as new object."""
        if self.nrows != self.ncols:
            raise TypeError("Inverse of non-square matrix not supported.")
        try:
            return Matrix(matrix=np.linalg.inv(self.matrix))  # type: ignore
        except np.linalg.LinAlgError:
            raise ZeroDivisionError

    def determinant(self) -> float:
        """Returns determinant of matrix, raises :class:`ZeroDivisionError`
        if matrix is singular.

        """
        return float(np.linalg.det(self.matrix))


class Solver(abc.ABC):
    @abc.abstractmethod
    def solve_matrix(self, B: MatrixData | NDArray) -> Matrix:
        ...

    @abc.abstractmethod
    def solve_vector(self, B: Iterable[float]) -> list[float]:
        ...


def quadratic_equation(a: float, b: float, c: float, abs_tol=1e-12) -> Sequence[float]:
    """Returns the solution for the quadratic equation ``a*x^2 + b*x + c = 0``.

    Returns 0-2 solutions as a tuple of floats.
    """
    if abs(a) < abs_tol:
        if abs(b) < abs_tol:
            return (-c,)
        return (-c / b,)
    try:
        discriminant = math.sqrt(b**2 - 4 * a * c)
    except ValueError:  # domain error, sqrt of a negative number
        return tuple()
    return ((-b + discriminant) / (2.0 * a)), ((-b - discriminant) / (2.0 * a))


# noinspection PyPep8Naming
def cubic_equation(a: float, b: float, c: float, d: float) -> Sequence[float]:
    """Returns the solution for the cubic equation ``a*x^3 + b*x^2 + c*x + d = 0``.

    Returns 0-3 solutions as a tuple of floats.
    """
    if abs(a) < 1e-12:
        try:
            return quadratic_equation(b, c, d)
        except ArithmeticError:  # complex solution
            return tuple()
    A = b / a
    B = c / a
    C = d / a
    AA = A * A
    A3 = A / 3.0

    Q = (3.0 * B - AA) / 9.0
    R = (9.0 * A * B - 27.0 * C - 2.0 * (AA * A)) / 54.0
    QQQ = Q * Q * Q
    D = QQQ + (R * R)  # polynomial discriminant

    if D >= 0.0:  # complex or duplicate roots
        sqrtD = math.sqrt(D)
        exp = 1.0 / 3.0
        S = math.copysign(1.0, R + sqrtD) * math.pow(abs(R + sqrtD), exp)
        T = math.copysign(1.0, R - sqrtD) * math.pow(abs(R - sqrtD), exp)
        ST = S + T
        if S - T:  # is complex
            return (-A3 + ST,)  # real root
        else:
            ST_2 = ST / 2.0
            return (
                -A3 + ST,  # real root
                -A3 - ST_2,  # real part of complex root
                -A3 - ST_2,  # real part of complex root
            )

    th = math.acos(R / math.sqrt(-QQQ))
    sqrtQ2 = math.sqrt(-Q) * 2.0
    return (
        sqrtQ2 * math.cos(th / 3.0) - A3,
        sqrtQ2 * math.cos((th + 2.0 * math.pi) / 3.0) - A3,
        sqrtQ2 * math.cos((th + 4.0 * math.pi) / 3.0) - A3,
    )


def numpy_matrix_solver(A: MatrixData | NDArray, B: MatrixData | NDArray) -> Matrix:
    """Solves the linear equation system given by a nxn Matrix A . x = B by the
    numpy.linalg.solve() function.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], ... [an1, an2, ..., ann]]
        B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m], ... [bn1, bn2, ..., bnm]]

    Raises:
        numpy.linalg.LinAlgError: singular matrix

    """
    mat_A = np.array(A, dtype=np.float64)
    mat_B = np.array(B, dtype=np.float64)
    return Matrix(matrix=np.linalg.solve(mat_A, mat_B))  # type: ignore


def numpy_vector_solver(A: MatrixData | NDArray, B: Iterable[float]) -> list[float]:
    """Solves the linear equation system given by a nxn Matrix A . x = B,
    right-hand side quantities as vector B with n elements by the numpy.linalg.solve()
    function.

    Args:
        A: matrix [[a11, a12, ..., a1n], [a21, a22, ..., a2n], ... [an1, an2, ..., ann]]
        B: vector [b1, b2, ..., bn]

    Raises:
        numpy.linalg.LinAlgError: singular matrix

    """
    mat_A = np.array(A, dtype=np.float64)
    mat_B = np.array([[float(v)] for v in B], dtype=np.float64)
    return list(np.ravel(np.linalg.solve(mat_A, mat_B)))


class NumpySolver(Solver):
    """Replaces in v1.2 the :class:`LUDecomposition` solver."""

    def __init__(self, A: MatrixData | NDArray) -> None:
        self.mat_A = np.array(A, dtype=np.float64)

    def solve_matrix(self, B: MatrixData | NDArray) -> Matrix:
        """
        Solves the linear equation system given by the nxn Matrix
        A . x = B, right-hand side quantities as nxm Matrix B.

        Args:
            B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m],
                ... [bn1, bn2, ..., bnm]]

        Raises:
            numpy.linalg.LinAlgError: singular matrix

        """
        mat_B = np.array(B, dtype=np.float64)
        return Matrix(matrix=np.linalg.solve(self.mat_A, mat_B))  # type: ignore

    def solve_vector(self, B: Iterable[float]) -> list[float]:
        """Solves the linear equation system given by the nxn Matrix
        A . x = B, right-hand side quantities as vector B with n elements.

        Args:
            B: vector [b1, b2, ..., bn]

        Raises:
            numpy.linalg.LinAlgError: singular matrix

        """
        mat_B = np.array([[float(v)] for v in B], dtype=np.float64)
        return list(np.ravel(np.linalg.solve(self.mat_A, mat_B)))


def tridiagonal_vector_solver(A: MatrixData, B: Iterable[float]) -> list[float]:
    """Solves the linear equation system given by a tri-diagonal nxn Matrix
    A . x = B, right-hand side quantities as vector B. Matrix A is diagonal
    matrix defined by 3 diagonals [-1 (a), 0 (b), +1 (c)].

    Note: a0 is not used but has to be present, cn-1 is also not used and must
    not be present.

    If an :class:`ZeroDivisionError` exception occurs, the equation system can
    possibly be solved by :code:`BandedMatrixLU(A, 1, 1).solve_vector(B)`

    Args:
        A: diagonal matrix [[a0..an-1], [b0..bn-1], [c0..cn-1]] ::

            [[b0, c0, 0, 0, ...],
            [a1, b1, c1, 0, ...],
            [0, a2, b2, c2, ...],
            ... ]

        B: iterable of floats [[b1, b1, ..., bn]

    Returns:
        list of floats

    Raises:
        ZeroDivisionError: singular matrix

    """
    a, b, c = [list(v) for v in A]
    return _solve_tridiagonal_matrix(a, b, c, list(B))


def tridiagonal_matrix_solver(
    A: MatrixData | NDArray, B: MatrixData | NDArray
) -> Matrix:
    """Solves the linear equation system given by a tri-diagonal nxn Matrix
    A . x = B, right-hand side quantities as nxm Matrix B. Matrix A is diagonal
    matrix defined by 3 diagonals [-1 (a), 0 (b), +1 (c)].

    Note: a0 is not used but has to be present, cn-1 is also not used and must
    not be present.

    If an :class:`ZeroDivisionError` exception occurs, the equation system
    can possibly be solved by :code:`BandedMatrixLU(A, 1, 1).solve_vector(B)`

    Args:
        A: diagonal matrix [[a0..an-1], [b0..bn-1], [c0..cn-1]] ::

            [[b0, c0, 0, 0, ...],
            [a1, b1, c1, 0, ...],
            [0, a2, b2, c2, ...],
            ... ]

        B: matrix [[b11, b12, ..., b1m],
                   [b21, b22, ..., b2m],
                   ...
                   [bn1, bn2, ..., bnm]]

    Returns:
        matrix as :class:`Matrix` object

    Raises:
        ZeroDivisionError: singular matrix

    """
    a, b, c = [list(v) for v in A]
    if not isinstance(B, Matrix):
        matrix_b = Matrix(matrix=[list(row) for row in B])
    else:
        matrix_b = cast(Matrix, B)
    if matrix_b.nrows != len(b):
        raise ValueError("Row count of matrices A and B has to match.")

    return Matrix(
        matrix=[_solve_tridiagonal_matrix(a, b, c, col) for col in matrix_b.cols()]
    ).transpose()


def _solve_tridiagonal_matrix(
    a: list[float], b: list[float], c: list[float], r: list[float]
) -> list[float]:
    """Solves the linear equation system given by a tri-diagonal
    Matrix(a, b, c) . x = r.

    Matrix configuration::

        [[b0, c0, 0, 0, ...],
        [a1, b1, c1, 0, ...],
        [0, a2, b2, c2, ...],
        ... ]

    Args:
        a: lower diagonal [a0 .. an-1], a0 is not used but has to be present
        b: central diagonal [b0 .. bn-1]
        c: upper diagonal [c0 .. cn-1], cn-1 is not used and must not be present
        r: right-hand side quantities

    Returns:
        vector x as list of floats

    Raises:
        ZeroDivisionError: singular matrix

    """
    n: int = len(a)
    u: list[float] = [0.0] * n
    gam: list[float] = [0.0] * n
    bet: float = b[0]
    u[0] = r[0] / bet
    for j in range(1, n):
        gam[j] = c[j - 1] / bet
        bet = b[j] - a[j] * gam[j]
        u[j] = (r[j] - a[j] * u[j - 1]) / bet

    for j in range((n - 2), -1, -1):
        u[j] -= gam[j + 1] * u[j + 1]
    return u


def banded_matrix(A: Matrix, check_all=True) -> tuple[Matrix, int, int]:
    """Transform matrix A into a compact banded matrix representation.
    Returns compact representation as :class:`Matrix` object and
    lower- and upper band count m1 and m2.

    Args:
        A: input :class:`Matrix`
        check_all: check all diagonals if ``True`` or abort testing
            after first all zero diagonal if ``False``.

    """
    m1, m2 = detect_banded_matrix(A, check_all)
    m = compact_banded_matrix(A, m1, m2)
    return m, m1, m2


def detect_banded_matrix(A: Matrix, check_all=True) -> tuple[int, int]:
    """Returns lower- and upper band count m1 and m2.

    Args:
        A: input :class:`Matrix`
        check_all: check all diagonals if ``True`` or abort testing
            after first all zero diagonal if ``False``.

    """

    def detect_m2() -> int:
        m2: int = 0
        for d in range(1, A.ncols):
            if any(A.diag(d)):
                m2 = d
            elif not check_all:
                break
        return m2

    def detect_m1() -> int:
        m1: int = 0
        for d in range(1, A.nrows):
            if any(A.diag(-d)):
                m1 = d
            elif not check_all:
                break
        return m1

    return detect_m1(), detect_m2()


def compact_banded_matrix(A: Matrix, m1: int, m2: int) -> Matrix:
    """Returns compact banded matrix representation as :class:`Matrix` object.

    Args:
        A: matrix to transform
        m1: lower band count, excluding main matrix diagonal
        m2: upper band count, excluding main matrix diagonal

    """
    if A.nrows != A.ncols:
        raise TypeError("Square matrix required.")

    m = Matrix()

    for d in range(m1, 0, -1):
        col = [0.0] * d
        col.extend(A.diag(-d))
        m.append_col(col)

    m.append_col(A.diag(0))

    for d in range(1, m2 + 1):
        col = A.diag(d)
        col.extend([0.0] * d)
        m.append_col(col)
    return m


class BandedMatrixLU(Solver):
    """Represents a LU decomposition of a compact banded matrix."""

    def __init__(self, A: Matrix, m1: int, m2: int):
        lu_decompose = _lu_decompose
        if USE_C_EXT:
            # import error shows an installation issue
            from ezdxf.acc.np_support import lu_decompose  # type: ignore

        self.m1: int = int(m1)
        self.m2: int = int(m2)
        self.upper, self.lower, self.index = lu_decompose(A.matrix, self.m1, self.m2)

    @property
    def nrows(self) -> int:
        """Count of matrix rows."""
        return self.upper.shape[0]

    def solve_vector(self, B: Iterable[float]) -> list[float]:
        """Solves the linear equation system given by the banded nxn Matrix
        A . x = B, right-hand side quantities as vector B with n elements.

        Args:
            B: vector [b1, b2, ..., bn]

        Returns:
            vector as list of floats

        """

        solve_vector_banded_matrix = _solve_vector_banded_matrix
        if USE_C_EXT:
            # import error shows an installation issue
            from ezdxf.acc.np_support import solve_vector_banded_matrix  # type: ignore

        x: NDArray = np.array(B, dtype=np.float64)
        if len(x) != self.nrows:
            raise ValueError(
                "Item count of vector B has to be equal to matrix row count."
            )
        return list(
            solve_vector_banded_matrix(
                x, self.upper, self.lower, self.index, self.m1, self.m2
            )
        )

    def solve_matrix(self, B: MatrixData | NDArray) -> Matrix:
        """
        Solves the linear equation system given by the banded nxn Matrix
        A . x = B, right-hand side quantities as nxm Matrix B.

        Args:
            B: matrix [[b11, b12, ..., b1m], [b21, b22, ..., b2m],
                ... [bn1, bn2, ..., bnm]]

        Returns:
            matrix as :class:`Matrix` object

        """
        matrix_b = Matrix(matrix=B)
        if matrix_b.nrows != self.nrows:
            raise ValueError("Row count of self and matrix B has to match.")

        return Matrix(
            matrix=[self.solve_vector(col) for col in matrix_b.cols()]
        ).transpose()


def _lu_decompose(A: NDArray, m1: int, m2: int) -> tuple[NDArray, NDArray, NDArray]:
    # upper triangle of LU decomposition
    upper: NDArray = np.array(A, dtype=np.float64)

    # lower triangle of LU decomposition
    n: int = upper.shape[0]
    lower: NDArray = np.zeros((n, m1), dtype=np.float64)
    index: NDArray = np.zeros((n,), dtype=np.int64)

    mm: int = m1 + m2 + 1
    l: int = m1
    for i in range(m1):
        for j in range(m1 - i, mm):
            upper[i][j - l] = upper[i][j]
        l -= 1
        for j in range(mm - l - 1, mm):
            upper[i][j] = 0.0

    l = m1
    for k in range(n):
        dum = upper[k][0]
        i = k
        if l < n:
            l += 1
        for j in range(k + 1, l):
            if abs(upper[j][0]) > abs(dum):
                dum = upper[j][0]
                i = j
        index[k] = i + 1
        if i != k:
            for j in range(mm):
                upper[k][j], upper[i][j] = upper[i][j], upper[k][j]

        for i in range(k + 1, l):
            dum = upper[i][0] / upper[k][0]
            lower[k][i - k - 1] = dum
            for j in range(1, mm):
                upper[i][j - 1] = upper[i][j] - dum * upper[k][j]
            upper[i][mm - 1] = 0.0
    return upper, lower, index


def _solve_vector_banded_matrix(
    x: NDArray,
    upper: NDArray,
    lower: NDArray,
    index: NDArray,
    m1: int,
    m2: int,
) -> NDArray:
    """Solves the linear equation system given by the banded nxn Matrix
    A . x = B, right-hand side quantities as vector B with n elements.

    Args:
        B: vector [b1, b2, ..., bn]

    Returns:
        vector as list of floats

    """
    n: int = upper.shape[0]
    if x.shape[0] != n:
        raise ValueError("Item count of vector B has to be equal to matrix row count.")

    al: NDArray = lower
    au: NDArray = upper
    mm: int = m1 + m2 + 1
    l: int = m1
    for k in range(n):
        j = index[k] - 1
        if j != k:
            x[k], x[j] = x[j], x[k]
        if l < n:
            l += 1
        for j in range(k + 1, l):
            x[j] -= al[k][j - k - 1] * x[k]

    l = 1
    for i in range(n - 1, -1, -1):
        dum = x[i]
        for k in range(1, l):
            dum -= au[i][k] * x[k + i]
        x[i] = dum / au[i][0]
        if l < mm:
            l += 1

    return x

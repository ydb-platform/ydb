# Copyright (c) 2018-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Tuple,
    Iterable,
    Sequence,
    TYPE_CHECKING,
    Iterator,
    Optional,
)
from functools import partial
import math
import random

if TYPE_CHECKING:
    from ezdxf.math import UVec, AnyVec

ABS_TOL = 1e-12
isclose = partial(math.isclose, abs_tol=ABS_TOL)

__all__ = ["Vec3", "Vec2"]


class Vec3:
    """Immutable 3D vector class.

    This class is optimized for universality not for speed.
    Immutable means you can't change (x, y, z) components after initialization::

        v1 = Vec3(1, 2, 3)
        v2 = v1
        v2.z = 7  # this is not possible, raises AttributeError
        v2 = Vec3(v2.x, v2.y, 7)  # this creates a new Vec3() object
        assert v1.z == 3  # and v1 remains unchanged


    :class:`Vec3` initialization:

        - ``Vec3()``, returns ``Vec3(0, 0, 0)``
        - ``Vec3((x, y))``, returns ``Vec3(x, y, 0)``
        - ``Vec3((x, y, z))``, returns ``Vec3(x, y, z)``
        - ``Vec3(x, y)``, returns ``Vec3(x, y, 0)``
        - ``Vec3(x, y, z)``, returns  ``Vec3(x, y, z)``

    Addition, subtraction, scalar multiplication and scalar division left and
    right-handed are supported::

        v = Vec3(1, 2, 3)
        v + (1, 2, 3) == Vec3(2, 4, 6)
        (1, 2, 3) + v == Vec3(2, 4, 6)
        v - (1, 2, 3) == Vec3(0, 0, 0)
        (1, 2, 3) - v == Vec3(0, 0, 0)
        v * 3 == Vec3(3, 6, 9)
        3 * v == Vec3(3, 6, 9)
        Vec3(3, 6, 9) / 3 == Vec3(1, 2, 3)
        -Vec3(1, 2, 3) == (-1, -2, -3)

    Comparison between vectors and vectors or tuples is supported::

        Vec3(1, 2, 3) < Vec3 (2, 2, 2)
        (1, 2, 3) < tuple(Vec3(2, 2, 2))  # conversion necessary
        Vec3(1, 2, 3) == (1, 2, 3)

        bool(Vec3(1, 2, 3)) is True
        bool(Vec3(0, 0, 0)) is False

    """

    __slots__ = ["_x", "_y", "_z"]

    def __init__(self, *args):
        self._x, self._y, self._z = self.decompose(*args)

    @property
    def x(self) -> float:
        """x-axis value"""
        return self._x

    @property
    def y(self) -> float:
        """y-axis value"""
        return self._y

    @property
    def z(self) -> float:
        """z-axis value"""
        return self._z

    @property
    def xy(self) -> Vec3:
        """Vec3 as ``(x, y, 0)``, projected on the xy-plane."""
        return self.__class__(self._x, self._y)

    @property
    def xyz(self) -> tuple[float, float, float]:
        """Vec3 as ``(x, y, z)`` tuple."""
        return self._x, self._y, self._z

    @property
    def vec2(self) -> Vec2:
        """Real 2D vector as :class:`Vec2` object."""
        return Vec2((self._x, self._y))

    def replace(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
    ) -> Vec3:
        """Returns a copy of vector with replaced x-, y- and/or z-axis."""
        if x is None:
            x = self._x
        if y is None:
            y = self._y
        if z is None:
            z = self._z
        return self.__class__(x, y, z)

    def round(self, ndigits=None) -> Vec3:
        """Returns a new vector where all components are rounded to `ndigits`.

        Uses standard Python :func:`round` function for rounding.
        """
        return self.__class__(
            round(self._x, ndigits),
            round(self._y, ndigits),
            round(self._z, ndigits),
        )

    @classmethod
    def list(cls, items: Iterable[UVec]) -> list[Vec3]:
        """Returns a list of :class:`Vec3` objects."""
        return list(cls.generate(items))

    @classmethod
    def tuple(cls, items: Iterable[UVec]) -> Sequence[Vec3]:
        """Returns a tuple of :class:`Vec3` objects."""
        return tuple(cls.generate(items))

    @classmethod
    def generate(cls, items: Iterable[UVec]) -> Iterator[Vec3]:
        """Returns an iterable of :class:`Vec3` objects."""
        return (cls(item) for item in items)

    @classmethod
    def from_angle(cls, angle: float, length: float = 1.0) -> Vec3:
        """Returns a :class:`Vec3` object from `angle` in radians in the
        xy-plane, z-axis = ``0``.
        """
        return cls(math.cos(angle) * length, math.sin(angle) * length, 0.0)

    @classmethod
    def from_deg_angle(cls, angle: float, length: float = 1.0) -> Vec3:
        """Returns a :class:`Vec3` object from `angle` in degrees in the
        xy-plane, z-axis = ``0``.
        """
        return cls.from_angle(math.radians(angle), length)

    @staticmethod
    def decompose(*args) -> Tuple[float, float, float]:  # cannot use "tuple" here!
        """Converts input into a (x, y, z) tuple.

        Valid arguments are:

            - no args: ``decompose()`` returns (0, 0, 0)
            - 1 arg: ``decompose(arg)``, `arg` is tuple or list, tuple has to be
              (x, y[, z]): ``decompose((x, y))`` returns (x, y, 0.)
            - 2 args: ``decompose(x, y)`` returns (x, y, 0)
            - 3 args: ``decompose(x, y, z)`` returns (x, y, z)

        Returns:
            (x, y, z) tuple

        (internal API)

        """
        length = len(args)
        if length == 0:
            return 0.0, 0.0, 0.0
        elif length == 1:
            data = args[0]
            if isinstance(data, Vec3):
                return data._x, data._y, data._z
            else:
                length = len(data)
                if length == 2:
                    x, y = data
                    z = 0.0
                elif length == 3:
                    x, y, z = data
                else:
                    raise TypeError
                return float(x), float(y), float(z)
        elif length == 2:
            x, y = args
            return float(x), float(y), 0.0
        elif length == 3:
            x, y, z = args
            return float(x), float(y), float(z)
        raise TypeError

    @classmethod
    def random(cls, length: float = 1) -> Vec3:
        """Returns a random vector."""
        x = random.uniform(-1, 1)
        y = random.uniform(-1, 1)
        z = random.uniform(-1, 1)
        return Vec3(x, y, z).normalize(length)

    def __str__(self) -> str:
        """Return ``'(x, y, z)'`` as string."""
        return "({0.x}, {0.y}, {0.z})".format(self)

    def __repr__(self) -> str:
        """Return ``'Vec3(x, y, z)'`` as string."""
        return "Vec3" + self.__str__()

    def __len__(self) -> int:
        """Returns always ``3``."""
        return 3

    def __hash__(self) -> int:
        """Returns hash value of vector, enables the usage of vector as key in
        ``set`` and ``dict``.
        """
        return hash(self.xyz)

    def copy(self) -> Vec3:
        """Returns a copy of vector as :class:`Vec3` object."""
        return self  # immutable!

    __copy__ = copy

    def __deepcopy__(self, memodict: dict) -> Vec3:
        """:func:`copy.deepcopy` support."""
        return self  # immutable!

    def __getitem__(self, index: int) -> float:
        """Support for indexing:

        - v[0] is v.x
        - v[1] is v.y
        - v[2] is v.z

        """
        if isinstance(index, slice):
            raise TypeError("slicing not supported")
        if index == 0:
            return self._x
        elif index == 1:
            return self._y
        elif index == 2:
            return self._z
        else:
            raise IndexError(f"invalid index {index}")

    def __iter__(self) -> Iterator[float]:
        """Returns iterable of x-, y- and z-axis."""
        yield self._x
        yield self._y
        yield self._z

    def __abs__(self) -> float:
        """Returns length (magnitude) of vector."""
        return self.magnitude

    @property
    def magnitude(self) -> float:
        """Length of vector."""
        return self.magnitude_square**0.5

    @property
    def magnitude_xy(self) -> float:
        """Length of vector in the xy-plane."""
        return math.hypot(self._x, self._y)

    @property
    def magnitude_square(self) -> float:
        """Square length of vector."""
        x, y, z = self._x, self._y, self._z
        return x * x + y * y + z * z

    @property
    def is_null(self) -> bool:
        """``True`` if all components are close to zero: ``Vec3(0, 0, 0)``.
        Has a fixed absolute testing tolerance of 1e-12!
        """
        return (
            abs(self._x) <= ABS_TOL
            and abs(self._y) <= ABS_TOL
            and abs(self._z) <= ABS_TOL
        )

    def is_parallel(
        self, other: Vec3, *, rel_tol: float = 1e-9, abs_tol: float = 1e-12
    ) -> bool:
        """Returns ``True`` if `self` and `other` are parallel to vectors."""
        v1 = self.normalize()
        v2 = other.normalize()
        return v1.isclose(v2, rel_tol=rel_tol, abs_tol=abs_tol) or v1.isclose(
            -v2, rel_tol=rel_tol, abs_tol=abs_tol
        )

    @property
    def spatial_angle(self) -> float:
        """Spatial angle between vector and x-axis in radians."""
        return math.acos(X_AXIS.dot(self.normalize()))

    @property
    def spatial_angle_deg(self) -> float:
        """Spatial angle between vector and x-axis in degrees."""
        return math.degrees(self.spatial_angle)

    @property
    def angle(self) -> float:
        """Angle between vector and x-axis in the xy-plane in radians."""
        return math.atan2(self._y, self._x)

    @property
    def angle_deg(self) -> float:
        """Returns angle of vector and x-axis in the xy-plane in degrees."""
        return math.degrees(self.angle)

    def orthogonal(self, ccw: bool = True) -> Vec3:
        """Returns orthogonal 2D vector, z-axis is unchanged.

        Args:
            ccw: counter-clockwise if ``True`` else clockwise

        """
        return (
            self.__class__(-self._y, self._x, self._z)
            if ccw
            else self.__class__(self._y, -self._x, self._z)
        )

    def lerp(self, other: UVec, factor=0.5) -> Vec3:
        """Returns linear interpolation between `self` and `other`.

        Args:
            other: end point as :class:`Vec3` compatible object
            factor: interpolation factor (0 = self, 1 = other,
                0.5 = mid point)

        """
        d = (self.__class__(other) - self) * float(factor)
        return self.__add__(d)

    def project(self, other: UVec) -> Vec3:
        """Returns projected vector of `other` onto `self`."""
        uv = self.normalize()
        return uv * uv.dot(other)

    def normalize(self, length: float = 1.0) -> Vec3:
        """Returns normalized vector, optional scaled by `length`."""
        return self.__mul__(length / self.magnitude)

    def reversed(self) -> Vec3:
        """Returns negated vector (-`self`)."""
        return self.__class__(-self._x, -self._y, -self._z)

    __neg__ = reversed

    def __bool__(self) -> bool:
        """Returns ``True`` if vector is not (0, 0, 0)."""
        return not self.is_null

    def isclose(
        self, other: UVec, *, rel_tol: float = 1e-9, abs_tol: float = 1e-12
    ) -> bool:
        """Returns ``True`` if `self` is close to `other`.
        Uses :func:`math.isclose` to compare all axis.

        Learn more about the :func:`math.isclose` function in
        `PEP 485 <https://www.python.org/dev/peps/pep-0485/>`_.

        """
        x, y, z = self.decompose(other)
        return (
            math.isclose(self._x, x, rel_tol=rel_tol, abs_tol=abs_tol)
            and math.isclose(self._y, y, rel_tol=rel_tol, abs_tol=abs_tol)
            and math.isclose(self._z, z, rel_tol=rel_tol, abs_tol=abs_tol)
        )

    def __eq__(self, other: UVec) -> bool:
        """Equal operator.

        Args:
            other: :class:`Vec3` compatible object
        """
        if not isinstance(other, Vec3):
            other = Vec3(other)
        return self.x == other.x and self.y == other.y and self.z == other.z

    def __lt__(self, other: UVec) -> bool:
        """Lower than operator.

        Args:
            other: :class:`Vec3` compatible object

        """
        x, y, z = self.decompose(other)
        if self._x == x:
            if self._y == y:
                return self._z < z
            else:
                return self._y < y
        else:
            return self._x < x

    def __add__(self, other: UVec) -> Vec3:
        """Add :class:`Vec3` operator: `self` + `other`."""
        x, y, z = self.decompose(other)
        return self.__class__(self._x + x, self._y + y, self._z + z)

    def __radd__(self, other: UVec) -> Vec3:
        """RAdd :class:`Vec3` operator: `other` + `self`."""
        return self.__add__(other)

    def __sub__(self, other: UVec) -> Vec3:
        """Sub :class:`Vec3` operator: `self` - `other`."""

        x, y, z = self.decompose(other)
        return self.__class__(self._x - x, self._y - y, self._z - z)

    def __rsub__(self, other: UVec) -> Vec3:
        """RSub :class:`Vec3` operator: `other` - `self`."""
        x, y, z = self.decompose(other)
        return self.__class__(x - self._x, y - self._y, z - self._z)

    def __mul__(self, other: float) -> Vec3:
        """Scalar Mul operator: `self` * `other`."""
        scalar = float(other)
        return self.__class__(self._x * scalar, self._y * scalar, self._z * scalar)

    def __rmul__(self, other: float) -> Vec3:
        """Scalar RMul operator: `other` * `self`."""
        return self.__mul__(other)

    def __truediv__(self, other: float) -> Vec3:
        """Scalar Div operator: `self` / `other`."""
        scalar = float(other)
        return self.__class__(self._x / scalar, self._y / scalar, self._z / scalar)

    @staticmethod
    def sum(items: Iterable[UVec]) -> Vec3:
        """Add all vectors in `items`."""
        s = NULLVEC
        for v in items:
            s += v
        return s

    def dot(self, other: UVec) -> float:
        """Dot operator: `self` . `other`

        Args:
            other: :class:`Vec3` compatible object
        """
        x, y, z = self.decompose(other)
        return self._x * x + self._y * y + self._z * z

    def cross(self, other: UVec) -> Vec3:
        """Cross operator: `self` x `other`

        Args:
            other: :class:`Vec3` compatible object
        """
        x, y, z = self.decompose(other)
        return self.__class__(
            self._y * z - self._z * y,
            self._z * x - self._x * z,
            self._x * y - self._y * x,
        )

    def distance(self, other: UVec) -> float:
        """Returns distance between `self` and `other` vector."""
        v = self.__class__(other)
        return v.__sub__(self).magnitude

    def angle_between(self, other: UVec) -> float:
        """Returns angle between `self` and `other` in radians. +angle is
        counter clockwise orientation.

        Args:
            other: :class:`Vec3` compatible object

        """
        cos_theta = self.normalize().dot(self.__class__(other).normalize())
        # avoid domain errors caused by floating point imprecision:
        if cos_theta < -1.0:
            cos_theta = -1.0
        elif cos_theta > 1.0:
            cos_theta = 1.0
        return math.acos(cos_theta)

    def angle_about(self, base: UVec, target: UVec) -> float:
        # (c) 2020 by Matt Broadway, MIT License
        """Returns counter-clockwise angle in radians about `self` from `base` to
        `target` when projected onto the plane defined by `self` as the normal
        vector.

        Args:
            base: base vector, defines angle 0
            target: target vector
        """
        x_axis = (base - self.project(base)).normalize()
        y_axis = self.cross(x_axis).normalize()
        target_projected_x = x_axis.dot(target)
        target_projected_y = y_axis.dot(target)
        return math.atan2(target_projected_y, target_projected_x) % math.tau

    def rotate(self, angle: float) -> Vec3:
        """Returns vector rotated about `angle` around the z-axis.

        Args:
            angle: angle in radians

        """
        v = self.__class__(self.x, self.y, 0.0)
        v = Vec3.from_angle(v.angle + angle, v.magnitude)
        return self.__class__(v.x, v.y, self.z)

    def rotate_deg(self, angle: float) -> Vec3:
        """Returns vector rotated about `angle` around the z-axis.

        Args:
            angle: angle in degrees

        """
        return self.rotate(math.radians(angle))


X_AXIS = Vec3(1, 0, 0)
Y_AXIS = Vec3(0, 1, 0)
Z_AXIS = Vec3(0, 0, 1)
NULLVEC = Vec3(0, 0, 0)


def distance(p1: UVec, p2: UVec) -> float:
    """Returns distance between points `p1` and `p2`.

    Args:
        p1: first point as :class:`Vec3` compatible object
        p2: second point as :class:`Vec3` compatible object

    """
    return Vec3(p1).distance(p2)


def lerp(p1: UVec, p2: UVec, factor: float = 0.5) -> Vec3:
    """Returns linear interpolation between points `p1` and `p2` as :class:`Vec3`.

    Args:
        p1: first point as :class:`Vec3` compatible object
        p2: second point as :class:`Vec3` compatible object
        factor:  interpolation factor (``0`` = `p1`, ``1`` = `p2`, ``0.5`` = mid point)

    """
    return Vec3(p1).lerp(p2, factor)


class Vec2:
    """Immutable 2D vector class.

    Args:
        v: vector object with :attr:`x` and :attr:`y` attributes/properties or a
           sequence of float ``[x, y, ...]`` or x-axis as float if argument `y`
           is not ``None``
        y: second float for :code:`Vec2(x, y)`

    :class:`Vec2` implements a subset of :class:`Vec3`.

    """

    __slots__ = ["x", "y"]

    def __init__(self, v=(0.0, 0.0), y=None) -> None:
        try:  # fast path for Vec2() and Vec3() or any object providing x and y attributes
            self.x = v.x
            self.y = v.y
        except AttributeError:
            if y is None:  # given one tuple
                self.x = float(v[0])
                self.y = float(v[1])
            else:  # two floats given
                self.x = float(v)
                self.y = float(y)

    @property
    def vec3(self) -> Vec3:
        """Returns a 3D vector."""
        return Vec3(self.x, self.y, 0)

    def round(self, ndigits=None) -> Vec2:
        """Returns a new vector where all components are rounded to `ndigits`.

        Uses standard Python :func:`round` function for rounding.
        """
        return self.__class__(round(self.x, ndigits), round(self.y, ndigits))

    @classmethod
    def list(cls, items: Iterable[UVec]) -> list[Vec2]:
        return list(cls.generate(items))

    @classmethod
    def tuple(cls, items: Iterable[UVec]) -> Sequence[Vec2]:
        """Returns a tuple of :class:`Vec3` objects."""
        return tuple(cls.generate(items))

    @classmethod
    def generate(cls, items: Iterable[UVec]) -> Iterator[Vec2]:
        return (cls(item) for item in items)

    @classmethod
    def from_angle(cls, angle: float, length: float = 1.0) -> Vec2:
        return cls(math.cos(angle) * length, math.sin(angle) * length)

    @classmethod
    def from_deg_angle(cls, angle: float, length: float = 1.0) -> Vec2:
        return cls.from_angle(math.radians(angle), length)

    def __str__(self) -> str:
        return "({0.x}, {0.y})".format(self)

    def __repr__(self) -> str:
        return "Vec2" + self.__str__()

    def __len__(self) -> int:
        return 2

    def __hash__(self) -> int:
        return hash((self.x, self.y))

    def copy(self) -> Vec2:
        return self.__class__((self.x, self.y))

    __copy__ = copy

    def __deepcopy__(self, memodict: dict) -> Vec2:
        try:
            return memodict[id(self)]
        except KeyError:
            v = self.copy()
            memodict[id(self)] = v
            return v

    def __getitem__(self, index: int) -> float:
        if isinstance(index, slice):
            raise TypeError("slicing not supported")
        if index == 0:
            return self.x
        elif index == 1:
            return self.y
        else:
            raise IndexError(f"invalid index {index}")

    def __iter__(self) -> Iterator[float]:
        yield self.x
        yield self.y

    def __abs__(self) -> float:
        return self.magnitude

    @property
    def magnitude(self) -> float:
        """Returns length of vector."""
        return math.hypot(self.x, self.y)

    @property
    def is_null(self) -> bool:
        return abs(self.x) <= ABS_TOL and abs(self.y) <= ABS_TOL

    @property
    def angle(self) -> float:
        """Angle of vector in radians."""
        return math.atan2(self.y, self.x)

    @property
    def angle_deg(self) -> float:
        """Angle of vector in degrees."""
        return math.degrees(self.angle)

    def orthogonal(self, ccw: bool = True) -> Vec2:
        """Orthogonal vector

        Args:
            ccw: counter-clockwise if ``True`` else clockwise

        """
        if ccw:
            return self.__class__(-self.y, self.x)
        else:
            return self.__class__(self.y, -self.x)

    def lerp(self, other: AnyVec, factor: float = 0.5) -> Vec2:
        """Linear interpolation between `self` and `other`.

        Args:
            other: target vector/point
            factor: interpolation factor (0=self, 1=other, 0.5=mid point)

        Returns: interpolated vector

        """
        x = self.x + (other.x - self.x) * factor
        y = self.y + (other.y - self.y) * factor
        return self.__class__(x, y)

    def project(self, other: AnyVec) -> Vec2:
        """Project vector `other` onto `self`."""
        uv = self.normalize()
        return uv * uv.dot(other)

    def normalize(self, length: float = 1.0) -> Vec2:
        return self.__mul__(length / self.magnitude)

    def reversed(self) -> Vec2:
        return self.__class__(-self.x, -self.y)

    __neg__ = reversed

    def __bool__(self) -> bool:
        return not self.is_null

    def isclose(
        self, other: AnyVec, *, rel_tol: float = 1e-9, abs_tol: float = 1e-12
    ) -> bool:
        if not isinstance(other, Vec2):
            other = Vec2(other)
        return math.isclose(
            self.x, other.x, rel_tol=rel_tol, abs_tol=abs_tol
        ) and math.isclose(self.y, other.y, rel_tol=rel_tol, abs_tol=abs_tol)

    def __eq__(self, other: UVec) -> bool:
        if not isinstance(other, Vec2):
            other = Vec2(other)
        return self.x == other.x and self.y == other.y

    def __lt__(self, other: UVec) -> bool:
        # accepts also tuples, for more convenience at testing
        x, y, *_ = other
        if self.x == x:
            return self.y < y
        else:
            return self.x < x

    def __add__(self, other: AnyVec) -> Vec2:
        try:
            return self.__class__(self.x + other.x, self.y + other.y)
        except AttributeError:
            raise TypeError("invalid argument")

    def __sub__(self, other: AnyVec) -> Vec2:
        try:
            return self.__class__(self.x - other.x, self.y - other.y)
        except AttributeError:
            raise TypeError("invalid argument")

    def __rsub__(self, other: AnyVec) -> Vec2:
        try:
            return self.__class__(other.x - self.x, other.y - self.y)
        except AttributeError:
            raise TypeError("invalid argument")

    def __mul__(self, other: float) -> Vec2:
        return self.__class__(self.x * other, self.y * other)

    def __rmul__(self, other: float) -> Vec2:
        return self.__class__(self.x * other, self.y * other)

    def __truediv__(self, other: float) -> Vec2:
        return self.__class__(self.x / other, self.y / other)

    def dot(self, other: AnyVec) -> float:
        return self.x * other.x + self.y * other.y

    def det(self, other: AnyVec) -> float:
        return self.x * other.y - self.y * other.x

    def distance(self, other: AnyVec) -> float:
        return math.hypot(self.x - other.x, self.y - other.y)

    def angle_between(self, other: AnyVec) -> float:
        """Calculate angle between `self` and `other` in radians. +angle is
        counter-clockwise orientation.

        """
        cos_theta = self.normalize().dot(other.normalize())
        # avoid domain errors caused by floating point imprecision:
        if cos_theta < -1.0:
            cos_theta = -1.0
        elif cos_theta > 1.0:
            cos_theta = 1.0
        return math.acos(cos_theta)

    def rotate(self, angle: float) -> Vec2:
        """Rotate vector around origin.

        Args:
            angle: angle in radians

        """
        return self.__class__.from_angle(self.angle + angle, self.magnitude)

    def rotate_deg(self, angle: float) -> Vec2:
        """Rotate vector around origin.

        Args:
            angle: angle in degrees

        Returns: rotated vector

        """
        return self.__class__.from_angle(
            self.angle + math.radians(angle), self.magnitude
        )

    @staticmethod
    def sum(items: Iterable[Vec2]) -> Vec2:
        """Add all vectors in `items`."""
        s = Vec2(0, 0)
        for v in items:
            s += v
        return s

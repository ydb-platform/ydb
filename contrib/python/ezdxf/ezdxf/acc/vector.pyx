# cython: language_level=3
# cython: c_api_binop_methods=True
# Copyright (c) 2020-2024, Manfred Moitzi
# License: MIT License
from typing import Iterable, List, Sequence, TYPE_CHECKING, Tuple, Iterator
from libc.math cimport fabs, sin, cos, M_PI, hypot, atan2, acos, sqrt, fmod
import random

cdef extern from "constants.h":
    const double ABS_TOL
    const double REL_TOL
    const double M_TAU

cdef double RAD2DEG = 180.0 / M_PI
cdef double DEG2RAD = M_PI / 180.0

if TYPE_CHECKING:
    from ezdxf.math import AnyVec, UVec

cdef bint isclose(double a, double b, double rel_tol, double abs_tol):
    # Has to match the Python implementation!
    cdef double diff = fabs(b - a)
    return diff <= fabs(rel_tol * b) or \
           diff <= fabs(rel_tol * a) or \
           diff <= abs_tol


cdef double normalize_rad_angle(double a):
    # Emulate the Python behavior of (a % math.tau)
    cdef double res = fmod(a, M_TAU)
    if res < 0.0:
        res += M_TAU
    return res

def _normalize_rad_angle(double angle):
    # just for testing
    return normalize_rad_angle(angle)

cdef double normalize_deg_angle(double a):
    # Emulate the Python behavior of (a % 360)
    cdef double res = fmod(a, 360.0)
    if res < 0.0:
        res += 360.0
    return res

def _normalize_deg_angle(double angle):
    # just for testing
    return normalize_deg_angle(angle)

cdef class Vec2:
    """ Immutable 2D vector.

    Init:

    - Vec2(vec2)
    - Vec2(vec3)
    - Vec2((x, y))
    - Vec2((x, y, z)), ignore z-axis
    - Vec2(x, y)
    - Vec2(x, y, z), ignore z-axis

    """

    def __cinit__(self, *args):
        cdef Py_ssize_t count = len(<tuple> args)

        if count == 0:  # fastest init - default constructor
            self.x = 0
            self.y = 0
            return

        if count == 1:
            arg = args[0]
            if isinstance(arg, Vec2):
                # fast init by Vec2()
                self.x = (<Vec2> arg).x
                self.y = (<Vec2> arg).y
                return
            if isinstance(arg, Vec3):
                # fast init by Vec3()
                self.x = (<Vec3> arg).x
                self.y = (<Vec3> arg).y
                return
            args = arg
            count = len(args)

        # ignore z-axis but raise error for 4 or more arguments
        if count > 3:
            raise TypeError('invalid argument count')

        # slow init by sequence
        self.x = args[0]
        self.y = args[1]

    def __reduce__(self):
        return Vec2, (self.x, self.y)

    @property
    def vec3(self) -> Vec3:
        return Vec3(self)

    def round(self, ndigits=None) -> Vec2:
        # only used for testing
        return Vec2(round(self.x, ndigits), round(self.y, ndigits))

    @staticmethod
    def list(items: Iterable[UVec]) -> List[Vec2]:
        return list(Vec2.generate(items))

    @staticmethod
    def tuple(items: Iterable[UVec]) -> Sequence[Vec2]:
        return tuple(Vec2.generate(items))

    @staticmethod
    def generate(items: Iterable[UVec]) -> Iterator[Vec2]:
        return (Vec2(item) for item in items)

    @staticmethod
    def from_angle(double angle, double length = 1.0) -> Vec2:
        return v2_from_angle(angle, length)

    @staticmethod
    def from_deg_angle(double angle, double length = 1.0) -> Vec2:
        return v2_from_angle(angle * DEG2RAD, length)

    def __str__(self) -> str:
        return f'({self.x}, {self.y})'

    def __repr__(self)-> str:
        return "Vec2" + self.__str__()

    def __len__(self) -> int:
        return 2

    def __hash__(self) -> int:
        return hash((self.x, self.y))

    def copy(self) -> Vec2:
        return self  # immutable

    def __copy__(self) -> Vec2:
        return self  # immutable

    def __deepcopy__(self, memodict: dict) -> Vec2:
        return self  # immutable

    def __getitem__(self, int index) -> float:
        if index == 0:
            return self.x
        elif index == 1:
            return self.y
        else:
            raise IndexError(f'invalid index {index}')

    def __iter__(self) -> Iterator[float]:
        yield self.x
        yield self.y

    def __abs__(self) -> float:
        return hypot(self.x, self.y)

    @property
    def magnitude(self) -> float:
        return hypot(self.x, self.y)

    @property
    def is_null(self) -> bool:
        return fabs(self.x) <= ABS_TOL and fabs(self.y) <= ABS_TOL

    @property
    def angle(self) -> float:
        return atan2(self.y, self.x)

    @property
    def angle_deg(self) -> float:
        return atan2(self.y, self.x) * RAD2DEG

    def orthogonal(self, ccw: bool = True) -> Vec2:
        return v2_ortho(self, ccw)

    def lerp(self, other: "AnyVec", double factor = 0.5) -> Vec2:
        cdef Vec2 o = Vec2(other)
        return v2_lerp(self, o, factor)

    def normalize(self, double length = 1.) -> Vec2:
        return v2_normalize(self, length)

    def project(self, other: AnyVec) -> Vec2:
        cdef Vec2 o = Vec2(other)
        return v2_project(self, o)

    def __neg__(self) -> Vec2:
        cdef Vec2 res = Vec2()
        res.x = -self.x
        res.y = -self.y
        return res

    reversed = __neg__

    def __bool__(self) -> bool:
        return self.x != 0 or self.y != 0

    def isclose(self, other: UVec, *, double rel_tol=REL_TOL,
                double abs_tol = ABS_TOL) -> bool:
        cdef Vec2 o = Vec2(other)
        return isclose(self.x, o.x, rel_tol, abs_tol) and \
               isclose(self.y, o.y, rel_tol, abs_tol)

    def __eq__(self, other: UVec) -> bool:
        if not isinstance(other, Vec2):
            other = Vec2(other)
        return self.x == other.x and self.y == other.y

    def __lt__(self, other) -> bool:
        cdef Vec2 o = Vec2(other)
        if self.x == o.x:
            return self.y < o.y
        else:
            return self.x < o.x

    def __add__(self, other: AnyVec) -> Vec2:
        if not isinstance(other, Vec2):
            other = Vec2(other)
        return v2_add(self, <Vec2> other)

    # __radd__ not supported for Vec2

    __iadd__ = __add__  # immutable

    def __sub__(self, other: AnyVec) -> Vec2:
        if not isinstance(other, Vec2):
            other = Vec2(other)
        return v2_sub(self, <Vec2> other)

    # __rsub__ not supported for Vec2

    __isub__ = __sub__  # immutable

    def __mul__(self, factor) -> Vec2:
        if isinstance(self, Vec2):
            return v2_mul(self, factor)
        elif isinstance(factor, Vec2):
            return v2_mul(<Vec2> factor, <double> self)
        else:
            return NotImplemented

    # Special Cython <(3.0) feature: __rmul__ == __mul__(factor, self)

    def __rmul__(self, double factor) -> Vec2:
        # for Cython >= 3.0
        return v2_mul(self, factor)

    __imul__ = __mul__  # immutable

    def __truediv__(self, double factor) -> Vec2:
        return v2_mul(self, 1.0 / factor)

    # __rtruediv__ not supported -> TypeError

    def dot(self, other: AnyVec) -> float:
        cdef Vec2 o = Vec2(other)
        return v2_dot(self, o)

    def det(self, other: AnyVec) -> float:
        cdef Vec2 o = Vec2(other)
        return v2_det(self, o)

    def distance(self, other: AnyVec) -> float:
        cdef Vec2 o = Vec2(other)
        return v2_dist(self, o)

    def angle_between(self, other: AnyVec) -> float:
        cdef Vec2 o = Vec2(other)
        return v2_angle_between(self, o)

    def rotate(self, double angle) -> Vec2:
        cdef double self_angle = atan2(self.y, self.x)
        cdef double magnitude = hypot(self.x, self.y)
        return v2_from_angle(self_angle + angle, magnitude)

    def rotate_deg(self, double angle) -> Vec2:
        return self.rotate(angle * DEG2RAD)

    @staticmethod
    def sum(items: Iterable[Vec2]) -> Vec2:
        cdef Vec2 res = Vec2()
        cdef Vec2 tmp
        res.x = 0.0
        res.y = 0.0
        for v in items:
            tmp = v
            res.x += tmp.x
            res.y += tmp.y
        return res


cdef Vec2 v2_add(Vec2 a, Vec2 b):
    res = Vec2()
    res.x = a.x + b.x
    res.y = a.y + b.y
    return res

cdef Vec2 v2_sub(Vec2 a, Vec2 b):
    res = Vec2()
    res.x = a.x - b.x
    res.y = a.y - b.y
    return res

cdef Vec2 v2_mul(Vec2 a, double factor):
    res = Vec2()
    res.x = a.x * factor
    res.y = a.y * factor
    return res

cdef double v2_dot(Vec2 a, Vec2 b):
    return a.x * b.x + a.y * b.y

cdef double v2_det(Vec2 a, Vec2 b):
    return a.x * b.y - a.y * b.x

cdef double v2_dist(Vec2 a, Vec2 b):
    return hypot(a.x - b.x, a.y - b.y)

cdef Vec2 v2_from_angle(double angle, double length):
    cdef Vec2 res = Vec2()
    res.x = cos(angle) * length
    res.y = sin(angle) * length
    return res

cdef double v2_angle_between(Vec2 a, Vec2 b) except -1000:
    cdef double cos_theta = v2_dot(v2_normalize(a, 1.0), v2_normalize(b, 1.0))
    # avoid domain errors caused by floating point imprecision:
    if cos_theta < -1.0:
        cos_theta = -1.0
    elif cos_theta > 1.0:
        cos_theta = 1.0
    return acos(cos_theta)

cdef Vec2 v2_normalize(Vec2 a, double length):
    cdef double factor = length / hypot(a.x, a.y)
    cdef Vec2 res = Vec2()
    res.x = a.x * factor
    res.y = a.y * factor
    return res

cdef Vec2 v2_lerp(Vec2 a, Vec2 b, double factor):
    cdef Vec2 res = Vec2()
    res.x = a.x + (b.x - a.x) * factor
    res.y = a.y + (b.y - a.y) * factor
    return res

cdef Vec2 v2_ortho(Vec2 a, bint ccw):
    cdef Vec2 res = Vec2()
    if ccw:
        res.x = -a.y
        res.y = a.x
    else:
        res.x = a.y
        res.y = -a.x
    return res

cdef Vec2 v2_project(Vec2 a, Vec2 b):
    cdef Vec2 uv = v2_normalize(a, 1.0)
    return v2_mul(uv, v2_dot(uv, b))

cdef bint v2_isclose(Vec2 a, Vec2 b, double rel_tol, double abs_tol):
    return isclose(a.x, b.x, rel_tol, abs_tol) and \
           isclose(a.y, b.y, rel_tol, abs_tol)


cdef class Vec3:
    """ Immutable 3D vector.

    Init:

    - Vec3()
    - Vec3(vec3)
    - Vec3(vec2)
    - Vec3((x, y))
    - Vec3((x, y, z))
    - Vec3(x, y)
    - Vec3(x, y, z)

    """

    def __cinit__(self, *args):
        cdef Py_ssize_t count = len(<tuple> args)
        if count == 0:  # fastest init - default constructor
            self.x = 0
            self.y = 0
            self.z = 0
            return

        if count == 1:
            arg0 = args[0]
            if isinstance(arg0, Vec3):
                # fast init by Vec3()
                self.x = (<Vec3> arg0).x
                self.y = (<Vec3> arg0).y
                self.z = (<Vec3> arg0).z
                return
            if isinstance(arg0, Vec2):
                # fast init by Vec2()
                self.x = (<Vec2> arg0).x
                self.y = (<Vec2> arg0).y
                self.z = 0
                return
            args = arg0
            count = len(args)

        if count > 3 or count < 2:
            raise TypeError('invalid argument count')

        # slow init by sequence
        self.x = args[0]
        self.y = args[1]
        if count > 2:
            self.z = args[2]
        else:
            self.z = 0.0

    def __reduce__(self):
        return Vec3, self.xyz

    @property
    def xy(self) -> Vec3:
        cdef Vec3 res = Vec3()
        res.x = self.x
        res.y = self.y
        return res

    @property
    def xyz(self) -> Tuple[float, float, float]:
        return self.x, self.y, self.z

    @property
    def vec2(self) -> Vec2:
        cdef Vec2 res = Vec2()
        res.x = self.x
        res.y = self.y
        return res

    def replace(self, x: float = None, y: float = None,
                z: float = None) -> Vec3:
        cdef Vec3 res = Vec3()
        res.x = self.x if x is None else x
        res.y = self.y if y is None else y
        res.z = self.z if z is None else z
        return res

    def round(self, ndigits: int | None = None) -> Vec3:
        return Vec3(
            round(self.x, ndigits),
            round(self.y, ndigits),
            round(self.z, ndigits),
        )

    @staticmethod
    def list(items: Iterable[UVec]) -> List[Vec3]:
        return list(Vec3.generate(items))

    @staticmethod
    def tuple(items: Iterable[UVec]) -> Sequence[Vec3]:
        return tuple(Vec3.generate(items))

    @staticmethod
    def generate(items: Iterable[UVec]) -> Iterator[Vec3]:
        return (Vec3(item) for item in items)

    @staticmethod
    def from_angle(double angle, double length = 1.0) -> Vec3:
        return v3_from_angle(angle, length)

    @staticmethod
    def from_deg_angle(double angle, double length = 1.0) -> Vec3:
        return v3_from_angle(angle * DEG2RAD, length)

    @staticmethod
    def random(double length = 1.0) -> Vec3:
        cdef Vec3 res = Vec3()
        uniform = random.uniform
        res.x = uniform(-1, 1)
        res.y = uniform(-1, 1)
        res.z = uniform(-1, 1)
        return v3_normalize(res, length)

    def __str__(self) -> str:
        return f'({self.x}, {self.y}, {self.z})'

    def __repr__(self)-> str:
        return "Vec3" + self.__str__()

    def __len__(self) -> int:
        return 3

    def __hash__(self) -> int:
        return hash(self.xyz)

    def copy(self) -> Vec3:
        return self  # immutable

    __copy__ = copy

    def __deepcopy__(self, memodict: dict) -> Vec3:
        return self  # immutable!

    def __getitem__(self, int index) -> float:
        if index == 0:
            return self.x
        elif index == 1:
            return self.y
        elif index == 2:
            return self.z
        else:
            raise IndexError(f'invalid index {index}')

    def __iter__(self) -> Iterator[float]:
        yield self.x
        yield self.y
        yield self.z

    def __abs__(self) -> float:
        return v3_magnitude(self)

    @property
    def magnitude(self) -> float:
        return v3_magnitude(self)

    @property
    def magnitude_xy(self) -> float:
        return hypot(self.x, self.y)

    @property
    def magnitude_square(self) -> float:
        return v3_magnitude_sqr(self)

    @property
    def is_null(self) -> bool:
        return fabs(self.x) <= ABS_TOL and fabs(self.y) <= ABS_TOL and \
               fabs(self.z) <= ABS_TOL

    def is_parallel(self, other: UVec, *, double rel_tol=REL_TOL,
                    double abs_tol = ABS_TOL) -> bool:
        cdef Vec3 o = Vec3(other)
        cdef Vec3 v1 = v3_normalize(self, 1.0)
        cdef Vec3 v2 = v3_normalize(o, 1.0)
        cdef Vec3 neg_v2 = v3_reverse(v2)
        return v3_isclose(v1, v2, rel_tol, abs_tol) or \
               v3_isclose(v1, neg_v2, rel_tol, abs_tol)

    @property
    def spatial_angle(self) -> float:
        return acos(v3_dot(<Vec3> X_AXIS, v3_normalize(self, 1.0)))

    @property
    def spatial_angle_deg(self) -> float:
        return self.spatial_angle * RAD2DEG

    @property
    def angle(self) -> float:
        return atan2(self.y, self.x)

    @property
    def angle_deg(self) -> float:
        return atan2(self.y, self.x) * RAD2DEG

    def orthogonal(self, ccw: bool = True) -> Vec3:
        return v3_ortho(self, ccw)

    def lerp(self, other: UVec, double factor=0.5) -> Vec3:
        if not isinstance(other, Vec3):
            other = Vec3(other)
        return v3_lerp(self, <Vec3> other, factor)

    def project(self, other: UVec) -> Vec3:
        if not isinstance(other, Vec3):
            other = Vec3(other)
        return v3_project(self, <Vec3> other)

    def normalize(self, double length = 1.) -> Vec3:
        return v3_normalize(self, length)

    def reversed(self) -> Vec3:
        return v3_reverse(self)

    def __neg__(self) -> Vec3:
        return v3_reverse(self)

    def __bool__(self) -> bool:
        return not self.is_null

    def isclose(self, other: UVec, *, double rel_tol = REL_TOL,
                double abs_tol = ABS_TOL) -> bool:
        if not isinstance(other, Vec3):
            other = Vec3(other)
        return v3_isclose(self, <Vec3> other, rel_tol, abs_tol)

    def __eq__(self, other: UVec) -> bool:
        if not isinstance(other, Vec3):
            other = Vec3(other)
        return self.x == other.x and self.y == other.y and self.z == other.z

    def __lt__(self, other: UVec) -> bool:
        if not isinstance(other, Vec3):
            other = Vec3(other)
        if self.x == (<Vec3> other).x:
            return self.y < (<Vec3> other).y
        else:
            return self.x < (<Vec3> other).x

    # Special Cython (<3.0) feature: __radd__ == __add__(other, self)
    def __add__(self, other) -> Vec3:
        if not isinstance(self, Vec3):
            # other is the real self
            return v3_add(Vec3(self), <Vec3> other)

        if not isinstance(other, Vec3):
            other = Vec3(other)
        return v3_add(<Vec3> self, <Vec3> other)

    __radd__ = __add__  # Cython >= 3.0

    __iadd__ = __add__  # immutable

    # Special Cython (<3.0) feature: __rsub__ == __sub__(other, self)
    def __sub__(self, other) -> Vec3:
        if not isinstance(self, Vec3):
            # other is the real self
            return v3_sub(Vec3(self), <Vec3> other)

        if not isinstance(other, Vec3):
            other = Vec3(other)
        return v3_sub(<Vec3> self, <Vec3> other)

    def __rsub__(self, other) -> Vec3:
        # for Cython >= 3.0
        return v3_sub(Vec3(other), <Vec3> self)

    __isub__ = __sub__  # immutable

    # Special Cython <(3.0) feature: __rmul__ == __mul__(factor, self)
    def __mul__(self, factor) -> Vec3:
        if isinstance(factor, Vec3):
            return v3_mul(<Vec3> factor, self)
        return v3_mul(<Vec3> self, factor)

    def __rmul__(self, double factor) -> Vec3:
        # for Cython >= 3.0
        return v3_mul(self, factor)

    __imul__ = __mul__  # immutable

    def __truediv__(self, double factor) -> Vec3:
        return v3_mul(self, 1.0 / factor)

    # __rtruediv__ not supported -> TypeError

    @staticmethod
    def sum(items: Iterable[UVec]) -> Vec3:
        cdef Vec3 res = Vec3()
        cdef Vec3 tmp
        for v in items:
            tmp = Vec3(v)
            res.x += tmp.x
            res.y += tmp.y
            res.z += tmp.z
        return res

    def dot(self, other: UVec) -> float:
        cdef Vec3 o = Vec3(other)
        return v3_dot(self, o)

    def cross(self, other: UVec) -> Vec3:
        cdef Vec3 o = Vec3(other)
        return v3_cross(self, o)

    def distance(self, other: UVec) -> float:
        cdef Vec3 o = Vec3(other)
        return v3_dist(self, o)

    def angle_between(self, other: UVec) -> float:
        cdef Vec3 o = Vec3(other)
        return v3_angle_between(self, o)

    def angle_about(self, base: UVec, target: UVec) -> float:
        cdef Vec3 b = Vec3(base)
        cdef Vec3 t = Vec3(target)
        return v3_angle_about(self, b, t)

    def rotate(self, double angle) -> Vec3:
        cdef double angle_ = atan2(self.y, self.x) + angle
        cdef double magnitude_ = hypot(self.x, self.y)
        cdef Vec3 res = Vec3.from_angle(angle_, magnitude_)
        res.z = self.z
        return res

    def rotate_deg(self, double angle) -> Vec3:
        return self.rotate(angle * DEG2RAD)


X_AXIS = Vec3(1, 0, 0)
Y_AXIS = Vec3(0, 1, 0)
Z_AXIS = Vec3(0, 0, 1)
NULLVEC = Vec3(0, 0, 0)

cdef Vec3 v3_add(Vec3 a, Vec3 b):
    res = Vec3()
    res.x = a.x + b.x
    res.y = a.y + b.y
    res.z = a.z + b.z
    return res

cdef Vec3 v3_sub(Vec3 a, Vec3 b):
    res = Vec3()
    res.x = a.x - b.x
    res.y = a.y - b.y
    res.z = a.z - b.z
    return res


cdef Vec3 v3_mul(Vec3 a, double factor):
    res = Vec3()
    res.x = a.x * factor
    res.y = a.y * factor
    res.z = a.z * factor
    return res


cdef Vec3 v3_reverse(Vec3 a):
    cdef Vec3 res = Vec3()
    res.x = -a.x
    res.y = -a.y
    res.z = -a.z
    return res


cdef double v3_dot(Vec3 a, Vec3 b):
    return a.x * b.x + a.y * b.y + a.z * b.z


cdef Vec3 v3_cross(Vec3 a, Vec3 b):
    res = Vec3()
    res.x = a.y * b.z - a.z * b.y
    res.y = a.z * b.x - a.x * b.z
    res.z = a.x * b.y - a.y * b.x
    return res


cdef inline double v3_magnitude_sqr(Vec3 a):
    return a.x * a.x + a.y * a.y + a.z * a.z


cdef inline double v3_magnitude(Vec3 a):
    return sqrt(v3_magnitude_sqr(a))


cdef double v3_dist(Vec3 a, Vec3 b):
    cdef double dx = a.x - b.x
    cdef double dy = a.y - b.y
    cdef double dz = a.z - b.z
    return sqrt(dx * dx + dy * dy + dz * dz)


cdef Vec3 v3_from_angle(double angle, double length):
    cdef Vec3 res = Vec3()
    res.x = cos(angle) * length
    res.y = sin(angle) * length
    return res


cdef double v3_angle_between(Vec3 a, Vec3 b) except -1000:
    cdef double cos_theta = v3_dot(v3_normalize(a, 1.0), v3_normalize(b, 1.0))
    # avoid domain errors caused by floating point imprecision:
    if cos_theta < -1.0:
        cos_theta = -1.0
    elif cos_theta > 1.0:
        cos_theta = 1.0
    return acos(cos_theta)


cdef double v3_angle_about(Vec3 a, Vec3 base, Vec3 target):
    cdef Vec3 x_axis = v3_normalize(v3_sub(base, v3_project(a, base)), 1.0)
    cdef Vec3 y_axis = v3_normalize(v3_cross(a, x_axis), 1.0)
    cdef double target_projected_x = v3_dot(x_axis, target)
    cdef double target_projected_y = v3_dot(y_axis, target)
    return normalize_rad_angle(atan2(target_projected_y, target_projected_x))


cdef Vec3 v3_normalize(Vec3 a, double length):
    cdef double factor = length / v3_magnitude(a)
    cdef Vec3 res = Vec3()
    res.x = a.x * factor
    res.y = a.y * factor
    res.z = a.z * factor
    return res


cdef Vec3 v3_lerp(Vec3 a, Vec3 b, double factor):
    cdef Vec3 res = Vec3()
    res.x = a.x + (b.x - a.x) * factor
    res.y = a.y + (b.y - a.y) * factor
    res.z = a.z + (b.z - a.z) * factor
    return res


cdef Vec3 v3_ortho(Vec3 a, bint ccw):
    cdef Vec3 res = Vec3()
    res.z = a.z
    if ccw:
        res.x = -a.y
        res.y = a.x
    else:
        res.x = a.y
        res.y = -a.x
    return res


cdef Vec3 v3_project(Vec3 a, Vec3 b):
    cdef Vec3 uv = v3_normalize(a, 1.0)
    return v3_mul(uv, v3_dot(uv, b))


cdef bint v3_isclose(Vec3 a, Vec3 b, double rel_tol, double abs_tol):
    return isclose(a.x, b.x, rel_tol, abs_tol) and \
           isclose(a.y, b.y, rel_tol, abs_tol) and \
           isclose(a.z, b.z, rel_tol, abs_tol)


def distance(p1: UVec, p2: UVec) -> float:
    cdef Vec3 a = Vec3(p1)
    cdef Vec3 b = Vec3(p2)
    return v3_dist(a, b)


def lerp(p1: UVec, p2: UVec, double factor = 0.5) -> Vec3:
    cdef Vec3 a = Vec3(p1)
    cdef Vec3 b = Vec3(p2)
    return v3_lerp(a, b, factor)

"""Affine transformation matrices

The Affine package is derived from Casey Duncan's Planar package. See the
copyright statement below.
"""

#############################################################################
# Copyright (c) 2010 by Casey Duncan
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * Neither the name(s) of the copyright holders nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AS IS AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#############################################################################

from collections import namedtuple
import math
import warnings


__all__ = ["Affine"]
__author__ = "Sean Gillies"
__version__ = "2.4.0"

EPSILON: float = 1e-5


class AffineError(Exception):
    pass


class TransformNotInvertibleError(AffineError):
    """The transform could not be inverted"""


class UndefinedRotationError(AffineError):
    """The rotation angle could not be computed for this transform"""


def cached_property(func):
    """Special property decorator that caches the computed
    property value in the object's instance dict the first
    time it is accessed.
    """
    name = func.__name__
    doc = func.__doc__

    def getter(self, name=name):
        try:
            return self.__dict__[name]
        except KeyError:
            self.__dict__[name] = value = func(self)
            return value

    getter.func_name = name
    return property(getter, doc=doc)


def cos_sin_deg(deg: float):
    """Return the cosine and sin for the given angle in degrees.

    With special-case handling of multiples of 90 for perfect right
    angles.
    """
    deg = deg % 360.0
    if deg == 90.0:
        return 0.0, 1.0
    elif deg == 180.0:
        return -1.0, 0
    elif deg == 270.0:
        return 0, -1.0
    rad = math.radians(deg)
    return math.cos(rad), math.sin(rad)


class Affine(namedtuple("Affine", ("a", "b", "c", "d", "e", "f", "g", "h", "i"))):
    """Two dimensional affine transform for 2D linear mapping.

    Parameters
    ----------
    a, b, c, d, e, f : float
        Coefficients of an augmented affine transformation matrix

        | x' |   | a  b  c | | x |
        | y' | = | d  e  f | | y |
        | 1  |   | 0  0  1 | | 1 |

        `a`, `b`, and `c` are the elements of the first row of the
        matrix. `d`, `e`, and `f` are the elements of the second row.

    Attributes
    ----------
    a, b, c, d, e, f, g, h, i : float
        The coefficients of the 3x3 augmented affine transformation
        matrix

        | x' |   | a  b  c | | x |
        | y' | = | d  e  f | | y |
        | 1  |   | g  h  i | | 1 |

        `g`, `h`, and `i` are always 0, 0, and 1.

    The Affine package is derived from Casey Duncan's Planar package.
    See the copyright statement below.  Parallel lines are preserved by
    these transforms. Affine transforms can perform any combination of
    translations, scales/flips, shears, and rotations.  Class methods
    are provided to conveniently compose transforms from these
    operations.

    Internally the transform is stored as a 3x3 transformation matrix.
    The transform may be constructed directly by specifying the first
    two rows of matrix values as 6 floats. Since the matrix is an affine
    transform, the last row is always ``(0, 0, 1)``.

    N.B.: multiplication of a transform and an (x, y) vector *always*
    returns the column vector that is the matrix multiplication product
    of the transform and (x, y) as a column vector, no matter which is
    on the left or right side. This is obviously not the case for
    matrices and vectors in general, but provides a convenience for
    users of this class.

    """

    precision = EPSILON

    def __new__(
        cls,
        a: float,
        b: float,
        c: float,
        d: float,
        e: float,
        f: float,
        g: float = 0.0,
        h: float = 0.0,
        i: float = 1.0,
    ):
        """Create a new object

        Parameters
        ----------
        a, b, c, d, e, f : float
            Elements of an augmented affine transformation matrix.
        """
        return tuple.__new__(
            cls,
            (
                a * 1.0,
                b * 1.0,
                c * 1.0,
                d * 1.0,
                e * 1.0,
                f * 1.0,
                g * 1.0,
                h * 1.0,
                i * 1.0,
            ),
        )

    @classmethod
    def from_gdal(cls, c: float, a: float, b: float, f: float, d: float, e: float):
        """Use same coefficient order as GDAL's GetGeoTransform().

        :param c, a, b, f, d, e: 6 floats ordered by GDAL.
        :rtype: Affine
        """
        return cls.__new__(cls, a, b, c, d, e, f)

    @classmethod
    def identity(cls):
        """Return the identity transform.

        :rtype: Affine
        """
        return identity

    @classmethod
    def translation(cls, xoff: float, yoff: float):
        """Create a translation transform from an offset vector.

        :param xoff: Translation x offset.
        :type xoff: float
        :param yoff: Translation y offset.
        :type yoff: float
        :rtype: Affine
        """
        return tuple.__new__(cls, (1.0, 0.0, xoff, 0.0, 1.0, yoff, 0.0, 0.0, 1.0))

    @classmethod
    def scale(cls, *scaling):
        """Create a scaling transform from a scalar or vector.

        :param scaling: The scaling factor. A scalar value will
            scale in both dimensions equally. A vector scaling
            value scales the dimensions independently.
        :type scaling: float or sequence
        :rtype: Affine
        """
        if len(scaling) == 1:
            sx = sy = float(scaling[0])
        else:
            sx, sy = scaling
        return tuple.__new__(cls, (sx, 0.0, 0.0, 0.0, sy, 0.0, 0.0, 0.0, 1.0))

    @classmethod
    def shear(cls, x_angle: float = 0, y_angle: float = 0):
        """Create a shear transform along one or both axes.

        :param x_angle: Shear angle in degrees parallel to the x-axis.
        :type x_angle: float
        :param y_angle: Shear angle in degrees parallel to the y-axis.
        :type y_angle: float
        :rtype: Affine
        """
        mx = math.tan(math.radians(x_angle))
        my = math.tan(math.radians(y_angle))
        return tuple.__new__(cls, (1.0, mx, 0.0, my, 1.0, 0.0, 0.0, 0.0, 1.0))

    @classmethod
    def rotation(cls, angle: float, pivot=None):
        """Create a rotation transform at the specified angle.

        A pivot point other than the coordinate system origin may be
        optionally specified.

        :param angle: Rotation angle in degrees, counter-clockwise
            about the pivot point.
        :type angle: float
        :param pivot: Point to rotate about, if omitted the rotation is
            about the origin.
        :type pivot: sequence
        :rtype: Affine
        """
        ca, sa = cos_sin_deg(angle)
        if pivot is None:
            return tuple.__new__(cls, (ca, -sa, 0.0, sa, ca, 0.0, 0.0, 0.0, 1.0))
        else:
            px, py = pivot
            return tuple.__new__(
                cls,
                (
                    ca,
                    -sa,
                    px - px * ca + py * sa,
                    sa,
                    ca,
                    py - px * sa - py * ca,
                    0.0,
                    0.0,
                    1.0,
                ),
            )

    @classmethod
    def permutation(cls, *scaling):
        """Create the permutation transform

        For 2x2 matrices, there is only one permutation matrix that is
        not the identity.

        :rtype: Affine
        """

        return tuple.__new__(cls, (0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0))

    def __str__(self) -> str:
        """Concise string representation."""
        return (
            "|% .2f,% .2f,% .2f|\n" "|% .2f,% .2f,% .2f|\n" "|% .2f,% .2f,% .2f|"
        ) % self

    def __repr__(self) -> str:
        """Precise string representation."""
        return ("Affine(%r, %r, %r,\n" "       %r, %r, %r)") % self[:6]

    def to_gdal(self):
        """Return same coefficient order as GDAL's SetGeoTransform().

        :rtype: tuple
        """
        return (self.c, self.a, self.b, self.f, self.d, self.e)

    def to_shapely(self):
        """Return an affine transformation matrix compatible with shapely

        Shapely's affinity module expects an affine transformation matrix
        in (a,b,d,e,xoff,yoff) order.

        :rtype: tuple
        """
        return (self.a, self.b, self.d, self.e, self.xoff, self.yoff)

    @property
    def xoff(self) -> float:
        """Alias for 'c'"""
        return self.c

    @property
    def yoff(self) -> float:
        """Alias for 'f'"""
        return self.f

    @cached_property
    def determinant(self) -> float:
        """The determinant of the transform matrix.

        This value is equal to the area scaling factor when the
        transform is applied to a shape.
        """
        a, b, c, d, e, f, g, h, i = self
        return a * e - b * d

    @property
    def _scaling(self):
        """The absolute scaling factors of the transformation.

        This tuple represents the absolute value of the scaling factors of the
        transformation, sorted from bigger to smaller.
        """
        a, b, _, d, e, _, _, _, _ = self

        # The singular values are the square root of the eigenvalues
        # of the matrix times its transpose, M M*
        # Computing trace and determinant of M M*
        trace = a ** 2 + b ** 2 + d ** 2 + e ** 2
        det = (a * e - b * d) ** 2

        delta = trace ** 2 / 4 - det
        if delta < 1e-12:
            delta = 0

        l1 = math.sqrt(trace / 2 + math.sqrt(delta))
        l2 = math.sqrt(trace / 2 - math.sqrt(delta))
        return l1, l2

    @property
    def eccentricity(self) -> float:
        """The eccentricity of the affine transformation.

        This value represents the eccentricity of an ellipse under
        this affine transformation.

        Raises NotImplementedError for improper transformations.
        """
        l1, l2 = self._scaling
        return math.sqrt(l1 ** 2 - l2 ** 2) / l1

    @property
    def rotation_angle(self) -> float:
        """The rotation angle in degrees of the affine transformation.

        This is the rotation angle in degrees of the affine transformation,
        assuming it is in the form M = R S, where R is a rotation and S is a
        scaling.

        Raises UndefinedRotationError for improper and degenerate
        transformations.
        """
        a, b, _, c, d, _, _, _, _ = self
        if self.is_proper or self.is_degenerate:
            l1, _ = self._scaling
            y, x = c / l1, a / l1
            return math.atan2(y, x) * 180 / math.pi
        else:
            raise UndefinedRotationError

    @property
    def is_identity(self) -> bool:
        """True if this transform equals the identity matrix,
        within rounding limits.
        """
        return self is identity or self.almost_equals(identity, self.precision)

    @property
    def is_rectilinear(self) -> bool:
        """True if the transform is rectilinear.

        i.e., whether a shape would remain axis-aligned, within rounding
        limits, after applying the transform.
        """
        a, b, c, d, e, f, g, h, i = self
        return (abs(a) < self.precision and abs(e) < self.precision) or (
            abs(d) < self.precision and abs(b) < self.precision
        )

    @property
    def is_conformal(self) -> bool:
        """True if the transform is conformal.

        i.e., if angles between points are preserved after applying the
        transform, within rounding limits.  This implies that the
        transform has no effective shear.
        """
        a, b, c, d, e, f, g, h, i = self
        return abs(a * b + d * e) < self.precision

    @property
    def is_orthonormal(self) -> bool:
        """True if the transform is orthonormal.

        Which means that the transform represents a rigid motion, which
        has no effective scaling or shear. Mathematically, this means
        that the axis vectors of the transform matrix are perpendicular
        and unit-length.  Applying an orthonormal transform to a shape
        always results in a congruent shape.
        """
        a, b, c, d, e, f, g, h, i = self
        return (
            self.is_conformal
            and abs(1.0 - (a * a + d * d)) < self.precision
            and abs(1.0 - (b * b + e * e)) < self.precision
        )

    @cached_property
    def is_degenerate(self) -> bool:
        """True if this transform is degenerate.

        Which means that it will collapse a shape to an effective area
        of zero. Degenerate transforms cannot be inverted.
        """
        return self.determinant == 0.0

    @cached_property
    def is_proper(self) -> bool:
        """True if this transform is proper.

        Which means that it does not include reflection.
        """
        return self.determinant > 0.0

    @property
    def column_vectors(self):
        """The values of the transform as three 2D column vectors"""
        a, b, c, d, e, f, _, _, _ = self
        return (a, d), (b, e), (c, f)

    def almost_equals(self, other, precision: float = EPSILON) -> bool:
        """Compare transforms for approximate equality.

        :param other: Transform being compared.
        :type other: Affine
        :return: True if absolute difference between each element
            of each respective transform matrix < ``self.precision``.
        """
        for i in (0, 1, 2, 3, 4, 5):
            if abs(self[i] - other[i]) >= precision:
                return False
        return True

    def __gt__(self, other) -> bool:
        return NotImplemented

    __ge__ = __lt__ = __le__ = __gt__

    # Override from base class. We do not support entrywise
    # addition, subtraction or scalar multiplication because
    # the result is not an affine transform

    def __add__(self, other):
        raise TypeError("Operation not supported")

    __iadd__ = __add__

    def __mul__(self, other):
        """Multiplication

        Apply the transform using matrix multiplication, creating
        a resulting object of the same type.  A transform may be applied
        to another transform, a vector, vector array, or shape.

        :param other: The object to transform.
        :type other: Affine, :class:`~planar.Vec2`,
            :class:`~planar.Vec2Array`, :class:`~planar.Shape`
        :rtype: Same as ``other``
        """
        sa, sb, sc, sd, se, sf, _, _, _ = self
        if isinstance(other, Affine):
            oa, ob, oc, od, oe, of, _, _, _ = other
            return tuple.__new__(
                self.__class__,
                (
                    sa * oa + sb * od,
                    sa * ob + sb * oe,
                    sa * oc + sb * of + sc,
                    sd * oa + se * od,
                    sd * ob + se * oe,
                    sd * oc + se * of + sf,
                    0.0,
                    0.0,
                    1.0,
                ),
            )
        else:
            try:
                vx, vy = other
                return (vx * sa + vy * sb + sc, vx * sd + vy * se + sf)
            except (ValueError, TypeError):
                return NotImplemented

    def __rmul__(self, other):
        """Right hand multiplication

        .. deprecated:: 2.3.0
            Right multiplication will be prohibited in version 3.0. This method
            will raise AffineError.

        Notes
        -----
        We should not be called if other is an affine instance This is
        just a guarantee, since we would potentially return the wrong
        answer in that case.
        """
        warnings.warn(
            "Right multiplication will be prohibited in version 3.0",
            DeprecationWarning,
            stacklevel=2,
        )
        assert not isinstance(other, Affine)
        return self.__mul__(other)

    def __imul__(self, other):
        if isinstance(other, Affine) or isinstance(other, tuple):
            return self.__mul__(other)
        else:
            return NotImplemented

    def itransform(self, seq) -> None:
        """Transform a sequence of points or vectors in place.

        :param seq: Mutable sequence of :class:`~planar.Vec2` to be
            transformed.
        :returns: None, the input sequence is mutated in place.
        """
        if self is not identity and self != identity:
            sa, sb, sc, sd, se, sf, _, _, _ = self
            for i, (x, y) in enumerate(seq):
                seq[i] = (x * sa + y * sb + sc, x * sd + y * se + sf)

    def __invert__(self):
        """Return the inverse transform.

        :raises: :except:`TransformNotInvertible` if the transform
            is degenerate.
        """
        if self.is_degenerate:
            raise TransformNotInvertibleError("Cannot invert degenerate transform")
        idet = 1.0 / self.determinant
        sa, sb, sc, sd, se, sf, _, _, _ = self
        ra = se * idet
        rb = -sb * idet
        rd = -sd * idet
        re = sa * idet
        return tuple.__new__(
            self.__class__,
            (ra, rb, -sc * ra - sf * rb, rd, re, -sc * rd - sf * re, 0.0, 0.0, 1.0),
        )

    __hash__ = tuple.__hash__  # hash is not inherited in Py 3

    def __getnewargs__(self):
        """Pickle protocol support

        Notes
        -----
        Normal unpickling creates a situation where __new__ receives all
        9 elements rather than the 6 that are required for the
        constructor.  This method ensures that only the 6 are provided.
        """
        return self.a, self.b, self.c, self.d, self.e, self.f


identity = Affine(1, 0, 0, 0, 1, 0)
"""The identity transform"""

# Miscellaneous utilities


def loadsw(s: str):
    """Returns Affine from the contents of a world file string.

    This method also translates the coefficients from center- to
    corner-based coordinates.

    :param s: str with 6 floats ordered in a world file.
    :rtype: Affine
    """
    if not hasattr(s, "split"):
        raise TypeError("Cannot split input string")
    coeffs = s.split()
    if len(coeffs) != 6:
        raise ValueError("Expected 6 coefficients, found %d" % len(coeffs))
    a, d, b, e, c, f = (float(x) for x in coeffs)
    center = tuple.__new__(Affine, [a, b, c, d, e, f, 0.0, 0.0, 1.0])
    return center * Affine.translation(-0.5, -0.5)


def dumpsw(obj) -> str:
    """Return string for a world file.

    This method also translates the coefficients from corner- to
    center-based coordinates.

    :rtype: str
    """
    center = obj * Affine.translation(0.5, 0.5)
    return "\n".join(repr(getattr(center, x)) for x in list("adbecf")) + "\n"

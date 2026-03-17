# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

__all__ = ("PdfMatrix", )

import math
import ctypes
import pypdfium2.raw as pdfium_c

# Note, the code below was written by a non-mathematician - might contain mistakes!
# In the future, we may want to consider adding a PdfRectangle support model to calculate size and corner points.

class PdfMatrix:
    """
    PDF transformation matrix helper class.
    
    See the PDF 1.7 specification, Section 8.3.3 ("Common Transformations").
    
    Note:
        * The PDF format uses row vectors.
        * Transformations operate from the origin of the coordinate system
          (PDF coordinates: commonly bottom left, but can be any corner in principle. Device coordinates: top left).
        * Matrix calculations are implemented independently in Python.
        * Matrix objects are immutable, so transforming methods return a new matrix.
        * Matrix objects implement ctypes auto-conversion to ``FS_MATRIX`` for easy use as C function parameter.
    
    Attributes:
        a (float): Matrix value [0][0].
        b (float): Matrix value [0][1].
        c (float): Matrix value [1][0].
        d (float): Matrix value [1][1].
        e (float): Matrix value [2][0] (X translation).
        f (float): Matrix value [2][1] (Y translation).
    """
    
    # See also pdfium/core/fxcrt/fx_coordinates.{h,cpp} (unfortunately, pdfium's matrix implementation is non-public)
    
    def __init__(self, a=1, b=0, c=0, d=1, e=0, f=0):
        self.a, self.b, self.c, self.d, self.e, self.f = a, b, c, d, e, f
    
    def __repr__(self):
        return f"PdfMatrix{self.get()}"
    
    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        return self.get() == other.get()
    
    @property
    def _as_parameter_(self):
        return ctypes.byref( self.to_raw() )
    
    
    def get(self):
        """
        Get the matrix as tuple of the form (a, b, c, d, e, f).
        """
        return (self.a, self.b, self.c, self.d, self.e, self.f)
    
    
    @classmethod
    def from_raw(cls, raw):
        """
        Load a :class:`.PdfMatrix` from a raw :class:`FS_MATRIX` object.
        """
        return cls(raw.a, raw.b, raw.c, raw.d, raw.e, raw.f)
    
    
    def to_raw(self):
        """
        Convert the matrix to a raw :class:`FS_MATRIX` object.
        """
        return pdfium_c.FS_MATRIX(*self.get())
    
    
    def multiply(self, other):
        """
        Multiply this matrix by another :class:`.PdfMatrix`, to concatenate transformations.
        """
        # M1 x M2 (self x other)
        # (a1, b1, 0)   (a2, b2, 0)   (a1a2+b1c2,    a1b2+b1d2,    0)
        # (c1, d1, 0) x (c2, d2, 0) = (c1a2+d1c2,    c1b2+d1d2,    0)
        # (e1, f1, 1)   (e2, f2, 1)   (e1a2+f1c2+e2, e1b2+f1d2+f2, 1)
        return PdfMatrix(
            a = self.a*other.a + self.b*other.c,
            b = self.a*other.b + self.b*other.d,
            c = self.c*other.a + self.d*other.c,
            d = self.c*other.b + self.d*other.d,
            # corresponds to: e, f = other.on_point(self.e, self.f) - transforms X/Y translation
            e = self.e*other.a + self.f*other.c + other.e,
            f = self.e*other.b + self.f*other.d + other.f,
        )
    
    
    def translate(self, x, y):
        """
        Parameters:
            x (float): Horizontal shift (<0: left, >0: right).
            y (float): Vertical shift.
        """
        # same as return PdfMatrix(self.a, self.b, self.c, self.d, self.e+x, self.f+y)
        return self.multiply( PdfMatrix(1, 0, 0, 1, x, y) )
    
    
    def scale(self, x, y):
        """
        Parameters:
            x (float): A factor to scale the X axis (<1: compress, >1: stretch).
            y (float): A factor to scale the Y axis.
        """
        # same as return PdfMatrix(self.a*x, self.b*y, self.c*x, self.d*y, self.e*x, self.f*y)
        return self.multiply( PdfMatrix(x, 0, 0, y) )
    
    
    def rotate(self, angle, ccw=False, rad=False):
        """
        Parameters:
            angle (float): Angle by which to rotate the matrix.
            ccw (bool): If True, rotate counter-clockwise.
            rad (bool): If True, interpret the angle as radians.
        """
        if not rad:
            angle = math.radians(angle)
        c, s = math.cos(angle), math.sin(angle)
        return self.multiply( PdfMatrix(c, s, -s, c) if ccw else PdfMatrix(c, -s, s, c) )
    
    
    def mirror(self, invert_x, invert_y):
        """
        Parameters:
            invert_x (bool): If True, invert X coordinates (horizontal transform). Corresponds to flipping around the Y axis.
            invert_y (bool): If True, invert Y coordinates (vertical transform). Corresponds to flipping around the X axis.
        Note:
            Flipping around a vertical axis leads to a horizontal transform, and vice versa.
        """
        return self.scale(x=(-1 if invert_x else 1), y=(-1 if invert_y else 1))
    
    
    def skew(self, x_angle, y_angle, rad=False):
        """
        Parameters:
            x_angle (float): Inner angle to skew the X axis.
            y_angle (float): Inner angle to skew the Y axis.
            rad (bool): If True, interpret the angles as radians.
        """
        if not rad:
            x_angle = math.radians(x_angle)
            y_angle = math.radians(y_angle)
        return self.multiply( PdfMatrix(1, math.tan(x_angle), math.tan(y_angle), 1) )
    
    
    def on_point(self, x, y):
        """
        Returns:
            (float, float): Transformed point.
        """
        # (x, y) -> (ax+cy+e, bx+dy+f)
        return (  # new point
            self.a*x + self.c*y + self.e,  # x
            self.b*x + self.d*y + self.f,  # y
        )
    
    
    def on_rect(self, left, bottom, right, top):
        """
        Returns:
            (float, float, float, float): Transformed rectangle.
        """
        points = (
            self.on_point(left, top),
            self.on_point(left, bottom),
            self.on_point(right, top),
            self.on_point(right, bottom),
        )
        return (  # new rect
            min(p[0] for p in points),  # left
            min(p[1] for p in points),  # bottom
            max(p[0] for p in points),  # right
            max(p[1] for p in points),  # top
        )

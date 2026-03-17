#############################################################################
# Planar is Copyright (c) 2010 by Casey Duncan
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

"""Transform unit tests"""

import math
import unittest
from textwrap import dedent

import pytest

import affine
from affine import Affine, EPSILON


def seq_almost_equal(t1, t2, error=0.00001):
    assert len(t1) == len(t2), f"{t1!r} != {t2!r}"
    for m1, m2 in zip(t1, t2):
        assert abs(m1 - m2) <= error, f"{t1!r} != {t2!r}"


class PyAffineTestCase(unittest.TestCase):
    def test_zero_args(self):
        with pytest.raises(TypeError):
            Affine()

    def test_wrong_arg_type(self):
        with pytest.raises(TypeError):
            Affine(None)

    def test_args_too_few(self):
        with pytest.raises(TypeError):
            Affine(1, 2)

    def test_args_too_many(self):
        with pytest.raises(TypeError):
            Affine(*range(10))

    def test_args_members_wrong_type(self):
        with pytest.raises(TypeError):
            Affine(0, 2, 3, None, None, "")

    def test_len(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        assert len(t) == 9

    def test_slice_last_row(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        assert t[-3:] == (0, 0, 1)

    def test_members_are_floats(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        for m in t:
            assert isinstance(m, float), repr(m)

    def test_getitem(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        assert t[0] == 1
        assert t[1] == 2
        assert t[2] == 3
        assert t[3] == 4
        assert t[4] == 5
        assert t[5] == 6
        assert t[6] == 0
        assert t[7] == 0
        assert t[8] == 1
        assert t[-1] == 1

    def test_getitem_wrong_type(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        with pytest.raises(TypeError):
            t["foobar"]

    def test_str(self):
        t = Affine(1.111, 2.222, 3.333, -4.444, -5.555, 6.666)
        assert str(t) == dedent(
            """\
            | 1.11, 2.22, 3.33|
            |-4.44,-5.55, 6.67|
            | 0.00, 0.00, 1.00|"""
        )

    def test_repr(self):
        t = Affine(1.111, 2.222, 3.456, 4.444, 5.5, 6.25)
        assert repr(t) == dedent(
            """\
            Affine(1.111, 2.222, 3.456,
                   4.444, 5.5, 6.25)"""
        )

    def test_identity_constructor(self):
        ident = Affine.identity()
        assert isinstance(ident, Affine)
        assert tuple(ident) == (1, 0, 0, 0, 1, 0, 0, 0, 1)
        assert ident.is_identity

    def test_permutation_constructor(self):
        perm = Affine.permutation()
        assert isinstance(perm, Affine)
        assert tuple(perm) == (0, 1, 0, 1, 0, 0, 0, 0, 1)
        assert (perm * perm).is_identity

    def test_translation_constructor(self):
        trans = Affine.translation(2, -5)
        assert isinstance(trans, Affine)
        assert tuple(trans) == (1, 0, 2, 0, 1, -5, 0, 0, 1)

    def test_scale_constructor(self):
        scale = Affine.scale(5)
        assert isinstance(scale, Affine)
        assert tuple(scale) == (5, 0, 0, 0, 5, 0, 0, 0, 1)
        scale = Affine.scale(-1, 2)
        assert tuple(scale) == (-1, 0, 0, 0, 2, 0, 0, 0, 1)
        assert tuple(Affine.scale(1)) == tuple(Affine.identity())

    def test_shear_constructor(self):
        shear = Affine.shear(30)
        assert isinstance(shear, Affine)
        mx = math.tan(math.radians(30))
        seq_almost_equal(tuple(shear), (1, mx, 0, 0, 1, 0, 0, 0, 1))
        shear = Affine.shear(-15, 60)
        mx = math.tan(math.radians(-15))
        my = math.tan(math.radians(60))
        seq_almost_equal(tuple(shear), (1, mx, 0, my, 1, 0, 0, 0, 1))
        shear = Affine.shear(y_angle=45)
        seq_almost_equal(tuple(shear), (1, 0, 0, 1, 1, 0, 0, 0, 1))

    def test_rotation_constructor(self):
        rot = Affine.rotation(60)
        assert isinstance(rot, Affine)
        r = math.radians(60)
        s, c = math.sin(r), math.cos(r)
        assert tuple(rot) == (c, -s, 0, s, c, 0, 0, 0, 1)
        rot = Affine.rotation(337)
        r = math.radians(337)
        s, c = math.sin(r), math.cos(r)
        seq_almost_equal(tuple(rot), (c, -s, 0, s, c, 0, 0, 0, 1))
        assert tuple(Affine.rotation(0)) == tuple(Affine.identity())

    def test_rotation_constructor_quadrants(self):
        assert tuple(Affine.rotation(0)) == (1, 0, 0, 0, 1, 0, 0, 0, 1)
        assert tuple(Affine.rotation(90)) == (0, -1, 0, 1, 0, 0, 0, 0, 1)
        assert tuple(Affine.rotation(180)) == (-1, 0, 0, 0, -1, 0, 0, 0, 1)
        assert tuple(Affine.rotation(-180)) == (-1, 0, 0, 0, -1, 0, 0, 0, 1)
        assert tuple(Affine.rotation(270)) == (0, 1, 0, -1, 0, 0, 0, 0, 1)
        assert tuple(Affine.rotation(-90)) == (0, 1, 0, -1, 0, 0, 0, 0, 1)
        assert tuple(Affine.rotation(360)) == (1, 0, 0, 0, 1, 0, 0, 0, 1)
        assert tuple(Affine.rotation(450)) == (0, -1, 0, 1, 0, 0, 0, 0, 1)
        assert tuple(Affine.rotation(-450)) == (0, 1, 0, -1, 0, 0, 0, 0, 1)

    def test_rotation_constructor_with_pivot(self):
        assert tuple(Affine.rotation(60)) == tuple(Affine.rotation(60, pivot=(0, 0)))
        rot = Affine.rotation(27, pivot=(2, -4))
        r = math.radians(27)
        s, c = math.sin(r), math.cos(r)
        assert tuple(rot) == (
            c,
            -s,
            2 - 2 * c - 4 * s,
            s,
            c,
            -4 - 2 * s + 4 * c,
            0,
            0,
            1,
        )
        assert tuple(Affine.rotation(0, (-3, 2))) == tuple(Affine.identity())

    def test_rotation_contructor_wrong_arg_types(self):
        with pytest.raises(TypeError):
            Affine.rotation(1, 1)

    def test_determinant(self):
        assert Affine.identity().determinant == 1
        assert Affine.scale(2).determinant == 4
        assert Affine.scale(0).determinant == 0
        assert Affine.scale(5, 1).determinant == 5
        assert Affine.scale(-1, 1).determinant == -1
        assert Affine.scale(-1, 0).determinant == 0
        assert Affine.rotation(77).determinant == pytest.approx(1)
        assert Affine.translation(32, -47).determinant == pytest.approx(1)

    def test_is_rectilinear(self):
        assert Affine.identity().is_rectilinear
        assert Affine.scale(2.5, 6.1).is_rectilinear
        assert Affine.translation(4, -1).is_rectilinear
        assert Affine.rotation(90).is_rectilinear
        assert not Affine.shear(4, -1).is_rectilinear
        assert not Affine.rotation(-26).is_rectilinear

    def test_is_conformal(self):
        assert Affine.identity().is_conformal
        assert Affine.scale(2.5, 6.1).is_conformal
        assert Affine.translation(4, -1).is_conformal
        assert Affine.rotation(90).is_conformal
        assert Affine.rotation(-26).is_conformal
        assert not Affine.shear(4, -1).is_conformal

    def test_is_orthonormal(self):
        assert Affine.identity().is_orthonormal
        assert Affine.translation(4, -1).is_orthonormal
        assert Affine.rotation(90).is_orthonormal
        assert Affine.rotation(-26).is_orthonormal
        assert not Affine.scale(2.5, 6.1).is_orthonormal
        assert not Affine.scale(0.5, 2).is_orthonormal
        assert not Affine.shear(4, -1).is_orthonormal

    def test_is_degenerate(self):
        assert not Affine.identity().is_degenerate
        assert not Affine.translation(2, -1).is_degenerate
        assert not Affine.shear(0, -22.5).is_degenerate
        assert not Affine.rotation(88.7).is_degenerate
        assert not Affine.scale(0.5).is_degenerate
        assert Affine.scale(0).is_degenerate
        assert Affine.scale(-10, 0).is_degenerate
        assert Affine.scale(0, 300).is_degenerate
        assert Affine.scale(0).is_degenerate
        assert Affine.scale(0).is_degenerate

    def test_column_vectors(self):
        a, b, c = Affine(2, 3, 4, 5, 6, 7).column_vectors
        assert isinstance(a, tuple)
        assert isinstance(b, tuple)
        assert isinstance(c, tuple)
        assert a == (2, 5)
        assert b == (3, 6)
        assert c == (4, 7)

    def test_almost_equals(self):
        EPSILON = 1e-5
        E = EPSILON * 0.5
        t = Affine(1.0, E, 0, -E, 1.0 + E, E)
        assert t.almost_equals(Affine.identity())
        assert Affine.identity().almost_equals(t)
        assert t.almost_equals(t)
        t = Affine(1.0, 0, 0, -EPSILON, 1.0, 0)
        assert not t.almost_equals(Affine.identity())
        assert not Affine.identity().almost_equals(t)
        assert t.almost_equals(t)

    def test_almost_equals_2(self):
        EPSILON = 1e-10
        E = EPSILON * 0.5
        t = Affine(1.0, E, 0, -E, 1.0 + E, E)
        assert t.almost_equals(Affine.identity(), precision=EPSILON)
        assert Affine.identity().almost_equals(t, precision=EPSILON)
        assert t.almost_equals(t, precision=EPSILON)
        t = Affine(1.0, 0, 0, -EPSILON, 1.0, 0)
        assert not t.almost_equals(Affine.identity(), precision=EPSILON)
        assert not Affine.identity().almost_equals(t, precision=EPSILON)
        assert t.almost_equals(t, precision=EPSILON)

    def test_equality(self):
        t1 = Affine(1, 2, 3, 4, 5, 6)
        t2 = Affine(6, 5, 4, 3, 2, 1)
        t3 = Affine(1, 2, 3, 4, 5, 6)
        assert t1 == t3
        assert not t1 == t2
        assert t2 == t2
        assert not t1 != t3
        assert not t2 != t2
        assert t1 != t2
        assert not t1 == 1
        assert t1 != 1

    def test_gt(self):
        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) > Affine(6, 5, 4, 3, 2, 1)

    def test_lt(self):
        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) < Affine(6, 5, 4, 3, 2, 1)

    def test_add(self):
        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) + Affine(6, 5, 4, 3, 2, 1)

    def test_sub(self):
        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) - Affine(6, 5, 4, 3, 2, 1)

    def test_mul_by_identity(self):
        t = Affine(1, 2, 3, 4, 5, 6)
        assert tuple(t * Affine.identity()) == tuple(t)

    def test_mul_transform(self):
        t = Affine.rotation(5) * Affine.rotation(29)
        assert isinstance(t, Affine)
        seq_almost_equal(t, Affine.rotation(34))
        t = Affine.scale(3, 5) * Affine.scale(2)
        seq_almost_equal(t, Affine.scale(6, 10))

    def test_itransform(self):
        pts = [(4, 1), (-1, 0), (3, 2)]
        r = Affine.scale(-2).itransform(pts)
        assert r is None, r
        assert pts == [(-8, -2), (2, 0), (-6, -4)]

        A = Affine.rotation(33)
        pts = [(4, 1), (-1, 0), (3, 2)]
        pts_expect = [A * pt for pt in pts]
        r = A.itransform(pts)
        assert r is None
        assert pts == pts_expect

    def test_mul_wrong_type(self):
        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) * None

    def test_mul_sequence_wrong_member_types(self):
        class NotPtSeq:
            @classmethod
            def from_points(cls, points):
                list(points)

            def __iter__(self):
                yield 0

        with pytest.raises(TypeError):
            Affine(1, 2, 3, 4, 5, 6) * NotPtSeq()

    def test_imul_transform(self):
        t = Affine.translation(3, 5)
        t *= Affine.translation(-2, 3.5)
        assert isinstance(t, Affine)
        seq_almost_equal(t, Affine.translation(1, 8.5))

    def test_inverse(self):
        seq_almost_equal(~Affine.identity(), Affine.identity())
        seq_almost_equal(~Affine.translation(2, -3), Affine.translation(-2, 3))
        seq_almost_equal(~Affine.rotation(-33.3), Affine.rotation(33.3))
        t = Affine(1, 2, 3, 4, 5, 6)
        seq_almost_equal(~t * t, Affine.identity())

    def test_cant_invert_degenerate(self):
        t = Affine.scale(0)
        with pytest.raises(affine.TransformNotInvertibleError):
            ~t

    def test_bad_type_world(self):
        """wrong type, i.e don't use readlines()"""
        with pytest.raises(TypeError):
            affine.loadsw(["1.0", "0.0", "0.0", "1.0", "0.0", "0.0"])

    def test_bad_value_world(self):
        """Wrong number of parameters."""
        with pytest.raises(ValueError):
            affine.loadsw("1.0\n0.0\n0.0\n1.0\n0.0\n0.0\n0.0")

    def test_simple_world(self):
        s = "1.0\n0.0\n0.0\n-1.0\n100.5\n199.5\n"
        a = affine.loadsw(s)
        assert a == Affine(1.0, 0.0, 100.0, 0.0, -1.0, 200.0)
        assert affine.dumpsw(a) == s

    def test_real_world(self):
        s = dedent(
            """\
                 39.9317755024
                 30.0907511581
                 30.0907511576
                -39.9317755019
            2658137.2266720217
            5990821.7039887439"""
        )  # no EOL
        a1 = affine.loadsw(s)
        assert a1.almost_equals(
            Affine(
                39.931775502364644,
                30.090751157602412,
                2658102.2154086917,
                30.090751157602412,
                -39.931775502364644,
                5990826.624500916,
            )
        )
        a1out = affine.dumpsw(a1)
        assert isinstance(a1out, str)
        a2 = affine.loadsw(a1out)
        assert a1.almost_equals(a2)


# We're using pytest for tests added after 1.0 and don't need unittest
# test case classes.


def test_gdal():
    t = Affine.from_gdal(-237481.5, 425.0, 0.0, 237536.4, 0.0, -425.0)
    assert t.c == t.xoff == -237481.5
    assert t.a == 425.0
    assert t.b == 0.0
    assert t.f == t.yoff == 237536.4
    assert t.d == 0.0
    assert t.e == -425.0
    assert tuple(t) == (425.0, 0.0, -237481.5, 0.0, -425.0, 237536.4, 0, 0, 1)
    assert t.to_gdal() == (-237481.5, 425.0, 0.0, 237536.4, 0.0, -425.0)


def test_shapely():
    t = Affine(425.0, 0.0, -237481.5, 0.0, -425.0, 237536.4)
    assert t.to_shapely() == (425.0, 0.0, 0.0, -425, -237481.5, 237536.4)


def test_imul_number():
    t = Affine(1, 2, 3, 4, 5, 6)
    try:
        t *= 2.0
    except TypeError:
        assert True


def test_mul_tuple():
    t = Affine(1, 2, 3, 4, 5, 6)
    t * (2.0, 2.0)


def test_rmul_tuple():
    with pytest.warns(DeprecationWarning):
        t = Affine(1, 2, 3, 4, 5, 6)
        (2.0, 2.0) * t


def test_transform_precision():
    t = Affine.rotation(45.0)
    assert t.precision == EPSILON
    t.precision = 1e-10
    assert t.precision == 1e-10
    assert Affine.rotation(0.0).precision == EPSILON


def test_associative():
    point = (12, 5)
    trans = Affine.translation(-10.0, -5.0)
    rot90 = Affine.rotation(90.0)
    result1 = rot90 * (trans * point)
    result2 = (rot90 * trans) * point
    seq_almost_equal(result1, (0.0, 2.0))
    seq_almost_equal(result1, result2)


def test_roundtrip():
    point = (12, 5)
    trans = Affine.translation(3, 4)
    rot37 = Affine.rotation(37.0)
    point_prime = (trans * rot37) * point
    roundtrip_point = ~(trans * rot37) * point_prime
    seq_almost_equal(point, roundtrip_point)


def test_eccentricity():
    assert Affine.identity().eccentricity == 0.0
    assert Affine.scale(2).eccentricity == 0.0
    # assert_equal(Affine.scale(0).eccentricity, ?)
    assert Affine.scale(2, 1).eccentricity == pytest.approx(math.sqrt(3) / 2)
    assert Affine.scale(2, 3).eccentricity == pytest.approx(math.sqrt(5) / 3)
    assert Affine.scale(1, 0).eccentricity == 1.0
    assert Affine.rotation(77).eccentricity == pytest.approx(0.0)
    assert Affine.translation(32, -47).eccentricity == pytest.approx(0.0)
    assert Affine.scale(-1, 1).eccentricity == pytest.approx(0.0)


def test_eccentricity_complex():
    assert (Affine.scale(2, 3) * Affine.rotation(77)).eccentricity == pytest.approx(
        math.sqrt(5) / 3
    )
    assert (Affine.rotation(77) * Affine.scale(2, 3)).eccentricity == pytest.approx(
        math.sqrt(5) / 3
    )
    assert (
        Affine.translation(32, -47) * Affine.rotation(77) * Affine.scale(2, 3)
    ).eccentricity == pytest.approx(math.sqrt(5) / 3)


def test_rotation_angle():
    assert Affine.identity().rotation_angle == 0.0
    assert Affine.scale(2).rotation_angle == 0.0
    assert Affine.scale(2, 1).rotation_angle == 0.0
    assert Affine.translation(32, -47).rotation_angle == pytest.approx(0.0)
    assert Affine.rotation(30).rotation_angle == pytest.approx(30)
    assert Affine.rotation(-150).rotation_angle == pytest.approx(-150)


def test_rotation_improper():
    with pytest.raises(affine.UndefinedRotationError):
        Affine.scale(-1, 1).rotation_angle


# See gh-71 for bug report motivating this test.
def test_mul_fallback_unpack():
    """Support fallback in case that other is a single object."""

    class TextPoint:
        """Not iterable, will trigger ValueError in Affine.__mul__."""

        def __rmul__(self, other):
            return other * (1, 2)

    assert Affine.identity() * TextPoint() == (1, 2)


# See gh-71 for bug report motivating this test.
def test_mul_fallback_type_error():
    """Support fallback in case that other is an unexpected type."""

    class TextPoint:
        """Iterable, but values trigger TypeError in Affine.__mul__."""

        def __iter__(self):
            return ("1", "2")

        def __rmul__(self, other):
            return other * (1, 2)

    assert Affine.identity() * TextPoint() == (1, 2)


if __name__ == "__main__":
    unittest.main()

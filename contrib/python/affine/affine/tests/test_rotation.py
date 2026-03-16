import math

from affine import Affine


def test_rotation_angle():
    """A positive angle rotates a vector counter clockwise

    (1.0, 0.0):

        |
        |
        |
        |
        0---------*

    Affine.rotation(45.0) * (1.0, 0.0) == (0.707..., 0.707...)

        |
        |      *
        |
        |
        0----------
    """
    x, y = Affine.rotation(45.0) * (1.0, 0.0)
    assert round(x, 14) == round(math.sqrt(2.0) / 2.0, 14)
    assert round(y, 14) == round(math.sqrt(2.0) / 2.0, 14)


def test_rotation_matrix():
    """A rotation matrix has expected elements

    | cos(a) -sin(a) |
    | sin(a)  cos(a) |

    """
    rot = Affine.rotation(90.0)
    assert round(rot.a, 15) == round(math.cos(math.pi / 2.0), 15)
    assert round(rot.b, 15) == round(-math.sin(math.pi / 2.0), 15)
    assert rot.c == 0.0
    assert round(rot.d, 15) == round(math.sin(math.pi / 2.0), 15)
    assert round(rot.e, 15) == round(math.cos(math.pi / 2.0), 15)
    assert rot.f == 0.0


def test_rotation_matrix_pivot():
    """A rotation matrix with pivot has expected elements"""
    rot = Affine.rotation(90.0, pivot=(1.0, 1.0))
    exp = (
        Affine.translation(1.0, 1.0)
        * Affine.rotation(90.0)
        * Affine.translation(-1.0, -1.0)
    )
    for r, e in zip(rot, exp):
        assert round(r, 15) == round(e, 15)

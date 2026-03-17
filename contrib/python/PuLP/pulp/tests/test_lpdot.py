from pulp import lpDot, LpVariable


def test_lpdot():
    x = LpVariable(name="x")

    product = lpDot(1, 2 * x)
    assert product.toDict() == [{"name": "x", "value": 2}]


def test_pulp_002():
    """
    Test the lpDot operation
    """
    x = LpVariable("x")
    y = LpVariable("y")
    z = LpVariable("z")
    a = [1, 2, 3]
    assert dict(lpDot([x, y, z], a)) == {x: 1, y: 2, z: 3}
    assert dict(lpDot([2 * x, 2 * y, 2 * z], a)) == {x: 2, y: 4, z: 6}
    assert dict(lpDot([x + y, y + z, z], a)) == {x: 1, y: 3, z: 5}
    assert dict(lpDot(a, [x + y, y + z, z])) == {x: 1, y: 3, z: 5}

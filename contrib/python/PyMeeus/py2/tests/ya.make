PY2TEST()

PEERDIR(
    contrib/python/PyMeeus
)

TEST_SRCS(
    __init__.py
    test_angle.py
    test_coordinates.py
    test_curvefitting.py
    test_earth.py
    test_epoch.py
    test_interpolation.py
    test_jupiter.py
    test_jupiterMoons.py
    test_mars.py
    test_mercury.py
    test_minor.py
    test_moon.py
    test_neptune.py
    test_pluto.py
    test_saturn.py
    test_sun.py
    test_uranus.py
    test_venus.py
)

NO_LINT()

END()

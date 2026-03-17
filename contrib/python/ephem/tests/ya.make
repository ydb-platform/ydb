PY3TEST()

PEERDIR(
    contrib/python/ephem
)

SRCDIR(
    contrib/python/ephem/ephem/tests
)

TEST_SRCS(
    __init__.py
    test_angles.py
    test_bodies.py
    test_cities.py
    test_constants.py
    test_dates.py
    test_github_issues.py
    test_jpl.py
    test_launchpad_236872.py
    test_launchpad_244811.py
    test_locales.py
    test_observers.py
    test_rst.py
    test_satellite.py
    test_stars.py
    test_usno.py
    test_usno_equinoxes.py
)

DATA (
    arcadia/contrib/python/ephem/ephem/tests/jpl
    arcadia/contrib/python/ephem/ephem/tests/usno
)

NO_LINT()

END()

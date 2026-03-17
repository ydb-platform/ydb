# Tests for the .mag visual magnitude attribute.

import ephem
from .wulfgar import add_test_functions, tc

def test_moon_mag_vs_horizons():
    # This test data comes from data/horizons_moon_magnitude.txt in the
    # PyEphem repository.

    expected_mags = [
        -5.416, -6.54, -7.529, -8.376, -9.097, -9.713, -10.242,
        -10.703, -11.113, -11.485, -11.83, -12.154, -12.457, -12.646,
        -12.389, -12.064, -11.737, -11.41, -11.079, -10.739, -10.38, -9.992,
        -9.562, -9.071, -8.498, -7.816, -6.994, -5.998, -4.83, -4.754,
        -6.015, -7.138, -8.082, -8.867, -9.519, -10.066, -10.53, -10.934,
        -11.294, -11.625, -11.935, -12.231, -12.511, -12.547, -12.246,
        -11.938, -11.633, -11.326, -11.014, -10.688, -10.338, -9.951, -9.51,
        -8.994, -8.373, -7.613, -6.673, -5.507, -4.182,
    ]
    d = ephem.Date('2025/1/1')
    m = ephem.Moon()
    delta = []
    for expected_mag in expected_mags:
        m.compute(d)
        delta.append(m.mag - expected_mag)
        d = ephem.Date(d + 1)

    n = len(delta)
    mean = sum(delta) / n
    variance = sum([(x - mean) ** 2 for x in delta]) / (n - 1)
    stddev = variance ** 0.5

    # Before the 4.2 Moon magnitude upgrade, the mean and stddev were:
    # -1.571457627118644 0.926306017842281
    # After:
    # -0.03349152542372886 0.06947472199231047
    # These asserts should keep the stats from regressing.

    tc.assertLess(abs(mean), 0.04)
    tc.assertLess(stddev, 0.07)

def load_tests(loader, tests, ignore):
    add_test_functions(loader, tests, __name__)
    return tests

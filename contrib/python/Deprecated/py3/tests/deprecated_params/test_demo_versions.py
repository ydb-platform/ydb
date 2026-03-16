# -*- coding: utf-8 -*-
"""
This example shows a function with an unused optional parameter. A warning
message should be emitted if `z` is used (as a positional or keyword parameter).
"""
import warnings

from deprecated.params import deprecated_params


class V2DeprecationWarning(DeprecationWarning):
    pass


# noinspection PyUnusedLocal
@deprecated_params(
    {
        "epsilon": "epsilon is deprecated in version v2",
        "start": "start is removed in version v2",
    },
    category=V2DeprecationWarning,
)
@deprecated_params("epsilon", reason="epsilon is deprecated in version v1.1")
def integrate(f, a, b, n=0, epsilon=0.0, start=None):
    epsilon = epsilon or (b - a) / n
    n = n or int((b - a) / epsilon)
    return sum((f(a + (i * epsilon)) + f(a + (i * epsilon) + epsilon)) * epsilon / 2 for i in range(n))


def test_only_one_warning_for_each_parameter():
    """
    This unit test checks that only one warning message is emitted for each deprecated parameter.

    However, we notice that the current implementation generates two warning messages for the `epsilon` parameter.
    We should therefore improve the implementation to avoid this.
    """
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        integrate(lambda x: x**2, 0, 2, epsilon=0.0012, start=123)
    actual = [{"message": str(w.message), "category": w.category} for w in warns]
    assert actual == [
        {"category": V2DeprecationWarning, "message": "epsilon is deprecated in version v2"},
        {"category": V2DeprecationWarning, "message": "start is removed in version v2"},
        {"category": DeprecationWarning, "message": "epsilon is deprecated in version v1.1"},
    ]

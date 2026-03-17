#!/usr/bin/env python

"""Tests for filesize humanizing."""
from __future__ import annotations

import pytest

import humanize


@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([300], "300 Bytes"),
        (["1000"], "1.0 kB"),
        ([10**3], "1.0 kB"),
        ([10**6], "1.0 MB"),
        ([10**9], "1.0 GB"),
        ([10**12], "1.0 TB"),
        ([10**15], "1.0 PB"),
        ([10**18], "1.0 EB"),
        ([10**21], "1.0 ZB"),
        ([10**24], "1.0 YB"),
        ([10**27], "1.0 RB"),
        ([10**30], "1.0 QB"),
        ([1000**1 * 31], "31.0 kB"),
        ([1000**2 * 32], "32.0 MB"),
        ([1000**3 * 33], "33.0 GB"),
        ([1000**4 * 34], "34.0 TB"),
        ([1000**5 * 35], "35.0 PB"),
        ([1000**6 * 36], "36.0 EB"),
        ([1000**7 * 37], "37.0 ZB"),
        ([1000**8 * 38], "38.0 YB"),
        ([1000**9 * 39], "39.0 RB"),
        ([1000**10 * 40], "40.0 QB"),
        ([300, True], "300 Bytes"),
        ([1024**1 * 31, True], "31.0 KiB"),
        ([1024**2 * 32, True], "32.0 MiB"),
        ([1024**3 * 33, True], "33.0 GiB"),
        ([1024**4 * 34, True], "34.0 TiB"),
        ([1024**5 * 35, True], "35.0 PiB"),
        ([1024**6 * 36, True], "36.0 EiB"),
        ([1024**7 * 37, True], "37.0 ZiB"),
        ([1024**8 * 38, True], "38.0 YiB"),
        ([1024**9 * 39, True], "39.0 RiB"),
        ([1024**10 * 40, True], "40.0 QiB"),
        ([1000**1 * 31, True], "30.3 KiB"),
        ([1000**2 * 32, True], "30.5 MiB"),
        ([1000**3 * 33, True], "30.7 GiB"),
        ([1000**4 * 34, True], "30.9 TiB"),
        ([1000**5 * 35, True], "31.1 PiB"),
        ([1000**6 * 36, True], "31.2 EiB"),
        ([1000**7 * 37, True], "31.3 ZiB"),
        ([1000**8 * 38, True], "31.4 YiB"),
        ([1000**9 * 39, True], "31.5 RiB"),
        ([1000**10 * 40, True], "31.6 QiB"),
        ([1000**1 * 31, False, True], "30.3K"),
        ([1000**2 * 32, False, True], "30.5M"),
        ([1000**3 * 33, False, True], "30.7G"),
        ([1000**4 * 34, False, True], "30.9T"),
        ([1000**5 * 35, False, True], "31.1P"),
        ([1000**6 * 36, False, True], "31.2E"),
        ([1000**7 * 37, False, True], "31.3Z"),
        ([1000**8 * 38, False, True], "31.4Y"),
        ([1000**9 * 39, False, True], "31.5R"),
        ([1000**10 * 40, False, True], "31.6Q"),
        ([300, False, True], "300B"),
        ([3000, False, True], "2.9K"),
        ([3000000, False, True], "2.9M"),
        ([1024, False, True], "1.0K"),
        ([1, False, False], "1 Byte"),
        ([1.0, False, False], "1 Byte"),
        (["1", False, False], "1 Byte"),
        ([3141592, False, False, "%.2f"], "3.14 MB"),
        ([3000, False, True, "%.3f"], "2.930K"),
        ([3000000000, False, True, "%.0f"], "3G"),
        ([10**26 * 30, True, False, "%.3f"], "2.423 RiB"),
        ([1.123456789], "1 Bytes"),
        ([1.123456789 * 10**3], "1.1 kB"),
        ([1.123456789 * 10**6], "1.1 MB"),
        ([1.123456789, False, True], "1B"),
        ([1.123456789 * 10**3, False, True], "1.1K"),
        ([1.123456789 * 10**6, False, True], "1.1M"),
    ],
)
def test_naturalsize(test_args: list[int] | list[int | bool], expected: str) -> None:
    assert humanize.naturalsize(*test_args) == expected

    # Retest with negative input
    if isinstance(test_args[0], int):
        test_args[0] *= -1
    else:
        test_args[0] = f"-{test_args[0]}"

    assert humanize.naturalsize(*test_args) == "-" + expected

# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
from contextlib import ExitStack

import matplotlib.pyplot as plt
import pytest


@pytest.fixture(autouse=True)
def mpl_test_cleanup(request):
    """Run tests in a context manager and close figures after each test."""
    with ExitStack() as stack:
        # At exit, close all open figures and switch backend back to original.
        stack.callback(plt.switch_backend, plt.get_backend())
        stack.callback(plt.close, 'all')

        # Run each test in a context manager so that state does not leak out
        plt.switch_backend('Agg')
        stack.enter_context(plt.rc_context())
        yield


def pytest_itemcollected(item):
    mpl_marker = item.get_closest_marker('mpl_image_compare')
    if mpl_marker is None:
        return

    # Matches old ImageTesting class default tolerance.
    mpl_marker.kwargs.setdefault('tolerance', 0.5)

    for path in item.fspath.parts(reverse=True):
        if path.basename == 'cartopy':
            return
        elif path.basename == 'tests':
            subdir = item.fspath.relto(path)[:-len(item.fspath.ext)]
            mpl_marker.kwargs.setdefault('baseline_dir',
                                         f'baseline_images/{subdir}')
            break

"""Fixtures for use with jupyter_client and downstream."""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import pytest

try:
    import ipykernel  # noqa: F401
    from jupyter_client.kernelspec import NATIVE_KERNEL_NAME
    from jupyter_client.manager import start_new_async_kernel
except ImportError:
    import warnings

    warnings.warn(
        "The client plugin has not been installed. "
        "If you're trying to use this plugin and you've installed "
        "`pytest-jupyter`, there is likely one more step "
        "you need. Try: `pip install 'pytest-jupyter[client]'`",
        stacklevel=2,
    )

# Bring in local plugins.
from pytest_jupyter.jupyter_core import *  # noqa: F403


@pytest.fixture
def jp_zmq_context():
    """Get a zmq context."""
    import zmq  # noqa: PLC0415

    ctx = zmq.asyncio.Context()
    yield ctx
    ctx.term()


@pytest.fixture
def jp_start_kernel(jp_environ, jp_asyncio_loop):
    """Get a function to a kernel and clean up resources when done."""
    kms = []
    kcs = []

    async def inner(kernel_name=NATIVE_KERNEL_NAME, **kwargs):
        """A function used to start a kernel."""
        km, kc = await start_new_async_kernel(kernel_name=kernel_name, **kwargs)
        kms.append(km)
        kcs.append(kc)
        return km, kc

    yield inner

    for kc in kcs:
        kc.stop_channels()

    for km in kms:
        jp_asyncio_loop.run_until_complete(km.shutdown_kernel(now=True))
        if not km.context.closed:
            raise AssertionError

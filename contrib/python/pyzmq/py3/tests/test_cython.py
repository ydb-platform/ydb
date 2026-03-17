import os
import sys

import pytest

import zmq

pyximport = pytest.importorskip("pyximport")

HERE = os.path.dirname(__file__)
cython_ext = os.path.join(HERE, "cython_ext.pyx")


@pytest.mark.skipif(
    not os.path.exists(cython_ext),
    reason=f"Requires cython test file {cython_ext}",
)
@pytest.mark.skipif(
    'zmq.backend.cython' not in sys.modules, reason="Requires cython backend"
)
@pytest.mark.skipif(
    sys.platform.startswith('win'), reason="Don't try runtime Cython on Windows"
)
@pytest.mark.skipif(
    os.environ.get("ZMQ_PREFIX") == "bundled", reason="bundled builds don't have zmq.h"
)
@pytest.mark.parametrize('language_level', [3, 2])
def test_cython(language_level, request, tmpdir):
    hook = pyximport.install(
        setup_args=dict(include_dirs=zmq.get_includes()),
        language_level=language_level,
        build_dir=str(tmpdir),
    )
    # don't actually need the hook, just the finder
    pyximport.uninstall(*hook)
    finder = hook[1]

    # loading the module tests the compilation
    spec = finder.find_spec("cython_ext", [HERE])
    cython_ext = spec.loader.create_module(spec)
    spec.loader.exec_module(cython_ext)

    assert hasattr(cython_ext, 'send_recv_test')

    # call the compiled function
    # this shouldn't do much
    msg = b'my msg'
    received = cython_ext.send_recv_test(msg)
    assert received == msg

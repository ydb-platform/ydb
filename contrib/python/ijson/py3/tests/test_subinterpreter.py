import importlib.util
import os
import pickle
import queue
import sys
import threading
import time

import pytest

try:
    from test.support import interpreters
    from test.support.interpreters import queues
except ImportError:
    pytest.skip("No sub-interpreter support", allow_module_level=True)


@pytest.fixture(name="interpreter")
def interpreter_fixture():
    interpreter = interpreters.create()
    yield interpreter
    interpreter.close()


def test_ijson_can_be_loaded(interpreter):
    interpreter.exec('import ijson')


def test_ijson_yajl2_backend_can_be_loaded(interpreter):
    spec = importlib.util.find_spec("ijson.backends._yajl2")
    if spec is None:
        pytest.skip("yajl2_c is not built")
    interpreter.exec('import ijson')
    interpreter.exec('ijson.get_backend("yajl2_c")')


def test_ijson_can_run(interpreter):

    queue = queues.create()
    VALUE = 43
    interpreter.prepare_main(queue_id=queue.id, value_in=VALUE)

    def ijson_in_subinterpreter():
        from test.support.interpreters.queues import Queue

        import ijson

        value_out = next(ijson.items(str(value_in).encode('ascii'), prefix=''))
        queue = Queue(queue_id)
        queue.put(value_out)

    thread = interpreter.call_in_thread(ijson_in_subinterpreter)
    value_out = queue.get(timeout=5)
    thread.join()

    assert VALUE == value_out

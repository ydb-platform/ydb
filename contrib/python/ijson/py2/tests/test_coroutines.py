from ijson import utils, compat

from .test_base import generate_test_cases


if compat.IS_PY2:
    def bytesiter(x):
        return x
else:
    def bytesiter(x):
        for b in x:
            yield bytes([b])


def get_all(routine, json_content, *args, **kwargs):
    events = utils.sendable_list()
    coro = routine(events, *args, **kwargs)
    for datum in bytesiter(json_content):
        coro.send(datum)
    coro.close()
    return events


def get_first(routine, json_content, *args, **kwargs):
    events = utils.sendable_list()
    coro = routine(events, *args, **kwargs)
    for datum in bytesiter(json_content):
        coro.send(datum)
        if events:
            return events[0]
    coro.close()
    if events:
        return events[0]
    return None

generate_test_cases(globals(), 'Coroutines', '_coro')
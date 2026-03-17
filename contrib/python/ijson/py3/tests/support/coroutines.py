from ijson import utils


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
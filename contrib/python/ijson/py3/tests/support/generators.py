import io


def _reader(json):
    if type(json) == bytes:
        return io.BytesIO(json)
    return io.StringIO(json)


def get_all(routine, json_content, *args, **kwargs):
    return list(routine(_reader(json_content), *args, **kwargs))

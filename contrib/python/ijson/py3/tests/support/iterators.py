import ijson


def _chunks(json, chunk_size=1):
    for i in range(0, len(json), chunk_size):
        yield json[i : i + chunk_size]


def get_all(routine, json_content, *args, **kwargs):
    reader = ijson.from_iter(_chunks(json_content))
    return list(routine(reader, *args, **kwargs))

import json
import math


class Float(float):
    def __eq__(self, obj) -> bool:
        if isinstance(obj, (float, int)):
            return math.isclose(self, obj)
        return super().__eq__(obj)

    # pylint: disable=useless-super-delegation
    def __hash__(self):
        return super().__hash__()


def json_loads(data, **kwargs):
    return json.loads(data, parse_float=Float, **kwargs)


def wrap_json(data):
    # bool is int, so make exception for bool
    if isinstance(data, bool):
        return data

    if isinstance(data, float):
        return Float(data)
    if isinstance(data, list):
        return [wrap_json(item) for item in data]
    if isinstance(data, dict):
        return {k: wrap_json(v) for k, v in data.items()}
    return data

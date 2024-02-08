try:
    from yt.packages.six import iteritems, text_type
except ImportError:
    from six import iteritems, text_type

try:
    from simplejson import load, dump, loads, dumps, JSONDecodeError  # noqa
except ImportError:
    # This version of simplejson has no compiled speedup module.
    from yt.packages.simplejson import load, dump, loads, dumps, JSONDecodeError  # noqa


def loads_as_bytes(*args, **kwargs):
    def encode(value):
        if isinstance(value, dict):
            return dict([(encode(k), encode(v)) for k, v in iteritems(value)])
        elif isinstance(value, list):
            return [encode(item) for item in value]
        elif isinstance(value, text_type):
            return value.encode("utf-8")
        else:
            return value

    return encode(loads(*args, **kwargs))

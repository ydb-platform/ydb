import json

from testsuite.utils import object_hook as object_hook_util


def loads(string, *args, **kwargs):
    """Helper function that wraps ``json.loads``.

    Automatically passes the object_hook for BSON type conversion.
    """

    object_hook = kwargs.pop('object_hook', None)
    kwargs['object_hook'] = object_hook_util.build_object_hook(
        object_hook=object_hook,
    )
    return json.loads(string, *args, **kwargs)


def substitute(json_obj, *, object_hook=None):
    """Create transformed json by making substitutions:

    {"$mockserver": "/path", "$schema": true} -> "http://localhost:9999/path"
    {"$dateDiff": 10} -> datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + timedelta(seconds=10)
    """
    hook = object_hook_util.build_object_hook(object_hook=object_hook)
    return object_hook_util.substitute(json_obj, hook)


def dumps(obj, *args, **kwargs):
    """Helper function that wraps ``json.dumps``.

    This function does NOT support ``bson.binary.Binary`` and
    ``bson.code.Code`` types. It just passes ``default`` argument to
    ``json.dumps`` function.
    """
    kwargs['ensure_ascii'] = kwargs.get('ensure_ascii', False)
    kwargs['sort_keys'] = kwargs.get('sort_keys', True)
    kwargs['indent'] = kwargs.get('indent', 2)
    kwargs['separators'] = kwargs.get('separators', (',', ': '))
    return json.dumps(obj, *args, **kwargs)

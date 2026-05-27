from . import mnc
from . import multinode
from . import agent

import yaml
import sys

from ydb.tools.mnc.lib.exceptions import SchemeError


modules = [mnc, multinode, agent]


def parse(path: str, scheme=None):
    obj = None
    with open(path, 'r') as yaml_template:
        obj = yaml.safe_load(yaml_template)
    if scheme is None:
        return obj
    obj, msgs = apply_scheme(obj, scheme)
    if msgs:
        raise SchemeError('\n'.join(msgs))
    return obj


def _safe_cast(obj, t):
    if not isinstance(t, type):
        return None, ['wrong cast, the second argument must be a type']
    try:
        if t == bool:
            if isinstance(obj, bool):
                return obj, []
            if isinstance(obj, str):
                normalized = obj.lower()
                if normalized in ('yes', 'true'):
                    return True, []
                if normalized in ('no', 'false'):
                    return False, []
            return None, [f'wrong cast, can\'t cast {obj} to type {t}']
        v = t(obj)
        return v, []
    except Exception:
        return None, [f'wrong cast, can\'t cast {obj} to type {t}']


def _is_optional(obj):
    return isinstance(obj, dict) and (obj.get('__optional__', False) or '__default__' in obj)


def _get_default(obj: dict):
    if '__default__' in obj:
        return obj['__default__']
    else:
        return None


def _apply_scheme_choices(obj, scheme):
    try:
        assert isinstance(scheme['__type__'], tuple)
        if obj not in scheme['__type__']:
            return None, [f'''obj not matched to choises: gotten '{obj}' but expected {scheme['__type__']}''']
        return obj, []
    except Exception as Ex:
        print(f"Error during processing next obj and scheme: {Ex}", obj, scheme, sep='\n', file=sys.stderr)
        raise


def _apply_scheme_primitive(obj, scheme):
    try:
        return _safe_cast(obj, scheme['__type__'])
    except Exception as Ex:
        print(f"Error during processing next obj and scheme: {Ex}", obj, scheme, sep='\n', file=sys.stderr)
        raise


def _apply_scheme_list(obj, scheme):
    try:
        if '__inner__' not in scheme:
            return None, [f"Wrong scheme, there isn't __inner__ {scheme}"]
        if not isinstance(obj, list):
            return None, [f'wrong cast, expected list, got {type(obj)}']
        result = []
        error_msgs = []
        for inner in obj:
            r, msgs = apply_scheme(inner, scheme['__inner__'])
            if r is None:
                result = None
            if result is not None:
                result.append(r)
            error_msgs += msgs
        return result, error_msgs
    except Exception as Ex:
        print(f"Error during processing next obj and scheme: {Ex}", obj, scheme, sep='\n', file=sys.stderr)
        raise


system_fields = ('__type__', '__optional__', '__default__', '__one_of__', '__additional_fields__', '__name__')


def _check_forbidden_fields(obj):
    msgs = []
    for key in system_fields:
        if key in obj:
            msgs.append(f"there is forbidden field '{key}' with value '{obj[key]}'")
    return not msgs, msgs


def _get_required_fields(scheme):
    return {
        k
        for k, v in scheme.items()
        if k not in system_fields and not _is_optional(v)
    }


def _get_all_fields(scheme):
    return {k for k in scheme if k not in system_fields}


def _check_one_of(obj, scheme):
    assert '__one_of__' in scheme

    msgs = []
    keys = set(obj)
    for key_list in scheme['__one_of__']:
        count = 0
        optional_key_list = False
        if isinstance(key_list, dict):
            optional_key_list = key_list['__optional__']
            key_list = key_list['__type__']
        for key in key_list:
            count += int(key in keys and obj and obj.get(key) is not None)
        if count == 0 and not optional_key_list:
            obj = None
            msgs.append(f"There aren't any of keys {key_list} {keys}")
        if count > 1:
            obj = None
            msgs.append(f"There are more than 1 key {key_list} {keys}")
    return obj, msgs


def _apply_scheme_object(obj, scheme):
    try:
        ok, msgs = _check_forbidden_fields(obj)
        if not ok:
            return None, msgs

        required_keys = _get_required_fields(scheme)
        all_keys = _get_all_fields(scheme)
        keys = set(obj)

        if len(required_keys - keys) != 0:
            return None, ["Not matching keys, required {0} given {1}".format(required_keys, keys)]
        if len(keys - all_keys) != 0 and not scheme.get('__additional_fields__', False):
            return None, ["There are excess keys, excess {0} all_keys {1} given {2}".format(keys - all_keys, all_keys, keys)]

        result = {k: _get_default(scheme[k]) for k in (all_keys - keys) if _is_optional(scheme[k])}

        error_msgs = []
        for key in keys:
            if scheme.get(key):
                r, msgs = apply_scheme(obj[key], scheme[key])
                if r is None:
                    result = None
            else:
                r, msgs = obj[key], []
            if result is not None:
                result[key] = r
            error_msgs += msgs

        if result is not None and '__one_of__' in scheme:
            result, msgs = _check_one_of(result, scheme)
            error_msgs += msgs
        return result, error_msgs
    except Exception as Ex:
        print(f"Error during processing next obj and scheme: {Ex}", obj, scheme, sep='\n', file=sys.stderr)
        raise


apply_func_for_type = {
    tuple: _apply_scheme_choices,
    int: _apply_scheme_primitive,
    str: _apply_scheme_primitive,
    float: _apply_scheme_primitive,
    bool: _apply_scheme_primitive,
    list: _apply_scheme_list,
    dict: _apply_scheme_object
}


def apply_scheme(obj, scheme):
    assert scheme is not None

    if not isinstance(scheme, dict):
        scheme = {'__type__': scheme}
    if '__type__' not in scheme:
        return None, [f"Wrong scheme, there isn't __type__ {scheme}"]

    if obj is None:
        return None, ["Value mustn't be None"]

    tp = scheme['__type__']
    if not isinstance(tp, type):
        tp = type(tp)

    if tp in apply_func_for_type:
        apply_func = apply_func_for_type[tp]
        return apply_func(obj, scheme)

    return None, [f"unexpected type {scheme['__type__']}"]

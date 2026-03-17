from functools import reduce, cmp_to_key
from math import prod
import regex


def get_functions(compile, build_function):
    sortable_types = {bool: 0, int: 1, float: 1, str: 2}
    other_types = 3

    def fn_get(*path: str):
        def getter(item):
            value = item

            for p in path:
                value_exists = value is not None and (
                    int(p) < len(value) if type(value) is list else p in value
                )
                value = value[p] if value_exists else None

            return value

        return getter

    def fn_pick(*properties):
        getters = {}
        for prop in properties:
            _, *path = prop
            name = path[-1]
            getters[name] = fn_get(*path)

        def pick(object):
            out = {}
            for key, getter in getters.items():
                out[key] = getter(object)
            return out

        return lambda data: list(map(pick, data)) if type(data) is list else pick(data)

    def fn_object(query):
        getters = {}
        for key, value in query.items():
            getters[key] = compile(value)

        def evaluate_object(data):
            obj = {}
            for obj_key, getter in getters.items():
                obj[obj_key] = getter(data)

            return obj

        return evaluate_object

    def fn_array(*items):
        getters = list(map(compile, items))

        return lambda data: list(map(lambda getter: getter(data), getters))

    def fn_filter(predicate):
        _predicate = compile(predicate)

        return (
            lambda data: raise_array_expected()
            if type(data) is not list
            else list(filter(lambda item: truthy(_predicate(item)), data))
        )

    def fn_map(callback):
        _callback = compile(callback)

        return (
            lambda data: raise_array_expected()
            if type(data) is not list
            else list(map(_callback, data))
        )

    def fn_map_object(callback):
        _callback = compile(callback)

        def map_object(data):
            object = {}

            for key, value in data.items():
                res = _callback({"key": key, "value": value})
                object[res["key"]] = res["value"]

            return object

        return map_object

    def fn_map_keys(callback):
        _callback = compile(callback)

        return lambda data: {_callback(key): value for key, value in data.items()}

    def fn_map_values(callback):
        _callback = compile(callback)

        return lambda data: {key: _callback(value) for key, value in data.items()}

    def fn_pipe(*entries):
        getters = list(map(compile, entries))

        return lambda data: reduce(lambda value, getter: getter(value), getters, data)

    def fn_sort(path=None, direction="asc"):
        getter = compile(path) if path is not None else lambda item: item
        sign = -1 if direction == "desc" else 1

        def compare(a, b):
            value_a = getter(a)
            value_b = getter(b)

            # Sort mixed types
            if type(value_a) is not type(value_b):
                index_a = sortable_types.get(type(value_a), other_types)
                index_b = sortable_types.get(type(value_b), other_types)

                return sign if index_a > index_b else -sign if index_b > index_a else 0

            # Sort two numbers, two strings, or two booleans
            if type(value_a) in sortable_types:
                return sign if value_a > value_b else -sign if value_b > value_a else 0

            # Leave other types like array or object ordered as is
            return 0

        return lambda data: sorted(data, key=cmp_to_key(compare))

    def fn_reverse():
        return lambda data: list(reversed(data))

    def fn_group_by(path):
        getter = compile(path)

        def group_by(data):
            res = {}

            for item in data:
                value = format(getter(item))
                if value in res:
                    res[value].append(item)
                else:
                    res[value] = [item]

            return res

        return group_by

    def fn_key_by(path):
        getter = compile(path)

        def key_by(data):
            res = {}

            for item in data:
                value = format(getter(item))
                if value not in res:
                    res[value] = item

            return res

        return key_by

    fn_flatten = lambda: lambda data: [x for xs in data for x in xs]
    fn_join = lambda separator="": lambda data: separator.join(data)
    fn_split = build_function(
        lambda text, separator=None: (
            text.split(separator) if separator != "" else split_chars(text)
        )
    )
    fn_substring = build_function(
        lambda text, start, end=None: text[max(start, 0) : end]
    )

    def contains(data, search_item):
        for item in data:
            if eq(item, search_item):
                return True

        return False

    def uniq(data):
        uniq_data = []

        for item in data:
            if not contains(uniq_data, item):
                uniq_data.append(item)

        return uniq_data

    fn_uniq = lambda: uniq
    fn_uniq_by = lambda path: lambda data: list(fn_key_by(path)(data).values())
    fn_limit = lambda count: lambda data: data[0:count] if count >= 0 else []
    fn_size = lambda: lambda data: len(data)
    fn_keys = lambda: lambda data: list(data.keys())
    fn_values = lambda: lambda data: list(data.values())

    fn_prod = (
        lambda: lambda data: raise_array_expected()
        if type(data) is not list
        else None
        if empty(data)
        else prod(data)
    )

    fn_sum = (
        lambda: lambda data: raise_array_expected()
        if type(data) is not list
        else sum(data)
    )

    fn_average = (
        lambda: lambda data: raise_array_expected()
        if type(data) is not list
        else None
        if empty(data)
        else sum(data) / len(data)
    )

    fn_max = (
        lambda: lambda data: raise_array_expected()
        if type(data) is not list
        else None
        if empty(data)
        else max(data)
    )

    fn_min = (
        lambda: lambda data: raise_array_expected()
        if type(data) is not list
        else None
        if empty(data)
        else min(data)
    )

    fn_and = build_function(
        lambda *args: reduce(lambda a, b: a and b, args) if not empty(args) else None
    )
    fn_or = build_function(
        lambda *args: reduce(lambda a, b: a or b, args) if args else None
    )
    fn_not = build_function(lambda a: not a)

    def fn_exists(query_get):
        _, *path = query_get

        def exec_exists(data):
            value = data

            for key in path:
                if value is None or key not in value:
                    return False
                value = value[key]

            return True

        return exec_exists

    def fn_if(condition, value_if_true, value_if_false):
        _condition = compile(condition)
        _value_if_true = compile(value_if_true)
        _value_if_false = compile(value_if_false)

        return (
            lambda data: _value_if_true(data)
            if truthy(_condition(data))
            else _value_if_false(data)
        )

    def fn_in(path, in_values):
        getter = compile(path)
        _values = list(map(compile, in_values))

        return lambda data: getter(data) in map(lambda _value: _value(data), _values)

    def fn_not_in(path, not_in_values):
        getter = compile(path)
        _values = list(map(compile, not_in_values))

        return lambda data: getter(data) not in map(
            lambda _value: _value(data), _values
        )

    def fn_regex(path, expression, options=None):
        compiled_regex = (
            regex.compile(expression, flags=_parse_regex_flags(options))
            if options
            else regex.compile(expression)
        )
        getter = compile(path)

        return lambda value: compiled_regex.match(getter(value)) is not None

    def match_to_json(result):
        value = result.group()
        groups = [*result.groups()]
        named_groups = result.groupdict()

        if named_groups:
            return {"value": value, "groups": groups, "namedGroups": named_groups}

        if groups:
            return {"value": value, "groups": groups}

        return {"value": value}

    def fn_match(path, expression, options=None):
        compiled_regex = (
            regex.compile(expression, flags=_parse_regex_flags(options))
            if options
            else regex.compile(expression)
        )
        getter = compile(path)

        def search(value):
            first_match = compiled_regex.search(getter(value))

            return match_to_json(first_match) if first_match else None

        return search

    def fn_match_all(path, expression, options=None):
        compiled_regex = (
            regex.compile(expression, flags=_parse_regex_flags(options))
            if options
            else regex.compile(expression)
        )
        getter = compile(path)

        return lambda value: [
            match_to_json(item) for item in compiled_regex.finditer(getter(value))
        ]

    def eq(a, b):
        return a == b and type(a) == type(b)

    def gt(a, b):
        return a > b if (type(a) is type(b)) and (type(a) in sortable_types) else False

    def lt(a, b):
        return a < b if (type(a) is type(b)) and (type(a) in sortable_types) else False

    fn_eq = build_function(eq)
    fn_gt = build_function(gt)
    fn_gte = build_function(lambda a, b: gt(a, b) or eq(a, b))
    fn_lt = build_function(lt)
    fn_lte = build_function(lambda a, b: lt(a, b) or eq(a, b))
    fn_ne = build_function(lambda a, b: not eq(a, b))

    fn_add = build_function(
        lambda a, b: to_string(a) + to_string(b)
        if type(a) is str or type(b) is str
        else a + b
    )
    fn_subtract = build_function(lambda a, b: a - b)
    fn_multiply = build_function(lambda a, b: a * b)
    fn_divide = build_function(lambda a, b: a / b)
    fn_pow = build_function(pow)
    fn_mod = build_function(lambda a, b: a % b)
    fn_abs = build_function(abs)
    fn_round = build_function(lambda value, digits=0: round(value, digits))
    fn_string = build_function(to_string)
    fn_number = build_function(to_number)

    return {
        "get": fn_get,
        "pick": fn_pick,
        "object": fn_object,
        "array": fn_array,
        "filter": fn_filter,
        "map": fn_map,
        "mapObject": fn_map_object,
        "mapKeys": fn_map_keys,
        "mapValues": fn_map_values,
        "pipe": fn_pipe,
        "sort": fn_sort,
        "reverse": fn_reverse,
        "groupBy": fn_group_by,
        "keyBy": fn_key_by,
        "flatten": fn_flatten,
        "join": fn_join,
        "split": fn_split,
        "substring": fn_substring,
        "uniq": fn_uniq,
        "uniqBy": fn_uniq_by,
        "limit": fn_limit,
        "size": fn_size,
        "keys": fn_keys,
        "values": fn_values,
        "prod": fn_prod,
        "sum": fn_sum,
        "average": fn_average,
        "min": fn_min,
        "max": fn_max,
        "and": fn_and,
        "or": fn_or,
        "not": fn_not,
        "exists": fn_exists,
        "if": fn_if,
        "in": fn_in,
        "not in": fn_not_in,
        "regex": fn_regex,
        "match": fn_match,
        "matchAll": fn_match_all,
        "eq": fn_eq,
        "gt": fn_gt,
        "gte": fn_gte,
        "lt": fn_lt,
        "lte": fn_lte,
        "ne": fn_ne,
        "add": fn_add,
        "subtract": fn_subtract,
        "multiply": fn_multiply,
        "divide": fn_divide,
        "pow": fn_pow,
        "mod": fn_mod,
        "abs": fn_abs,
        "round": fn_round,
        "string": fn_string,
        "number": fn_number,
    }


def _parse_regex_flags(flags):
    if flags is None or len(flags) == 0:
        return None

    all_flags = {
        "A": regex.A,
        "I": regex.I,
        "M": regex.M,
        "S": regex.S,
        "X": regex.X,
        "L": regex.L,
    }

    first, *rest = flags.upper()

    return reduce(
        lambda combined, flag: combined | all_flags[flag], rest, all_flags[first]
    )


def truthy(value):
    return value not in [False, 0, None]


def to_string(value):
    return (
        "false"
        if value is False
        else "true"
        if value is True
        else "null"
        if value is None
        else format(value)
    )


def to_number(value):
    try:
        return float(value)
    except ValueError:
        return None


def split_chars(text):
    (*chars,) = text
    return chars


def empty(array):
    return len(array) == 0


def raise_array_expected():
    raise RuntimeError("Array expected")


def raise_runtime_error(message: str):
    raise RuntimeError(message)


def validate_list(data):
    if type(data) is not list:
        raise_array_expected()
    return data

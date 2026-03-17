import copy


def order(source, paths):
    sort_root = '' in paths
    result = _order(source, paths)

    if sort_root and isinstance(result, (list, tuple)):
        result = sorted(result, key=_sort_key)

    return result


def assert_eq(first_source, second_source, paths):
    assert order(first_source, paths) == order(second_source, paths)


def _order(source, paths):
    if isinstance(source, list):
        return [_order(value, paths) for value in source]
    elif isinstance(source, dict):
        source_copy = copy.copy(source)
        for path in sorted(paths, reverse=True):
            head_tail = path.split('.', 1)
            head = head_tail[0]
            if head not in source_copy:
                continue
            value = source_copy[head]
            if len(head_tail) == 1:
                if isinstance(value, (list, tuple)):
                    source_copy[head] = sorted(value, key=_sort_key)
            else:
                source_copy[head] = _order(value, [head_tail[1]])
        return source_copy

    return source


def _sort_key(value):
    if isinstance(value, dict):
        return (
            type(value).__name__,
            [
                (k, _sort_key(v))
                for k, v in sorted(value.items(), key=_sort_key)
            ],
        )
    if isinstance(value, (list, tuple)):
        return (
            type(value).__name__,
            [_sort_key(v) for v in sorted(value, key=_sort_key)],
        )
    return (type(value).__name__, value)

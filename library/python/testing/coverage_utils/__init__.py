import re


def make_filter(prefix_filter, exclude_regexp):
    filters = []
    if prefix_filter:
        filters.append(lambda x: x.startswith(prefix_filter))
    if exclude_regexp:
        regexp = re.compile(exclude_regexp)
        filters.append(lambda x: not regexp.search(x))

    if filters:
        return lambda x: all(pred(x) for pred in filters)
    return lambda x: True

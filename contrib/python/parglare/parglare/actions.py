"""
Common parsing actions.
"""

import contextlib


def pass_none(_, value, *args):
    return None


def pass_nochange(_, value, *args):
    return value


def pass_empty(_, value, *args):
    """
    Used for EMPTY production alternative in collect.
    """
    return []


def pass_single(_, nodes):
    """
    Unpack single value and pass up.
    """
    return nodes[0]


def pass_inner(_, nodes):
    """
    Pass inner value up, e.g. for stripping parentheses as in
    `( <some expression> )`.
    """
    n = nodes[1:-1]
    with contextlib.suppress(ValueError):
        (n,) = n
    return n


def collect_first(_, nodes):
    """
    Used for:
    Elements = Elements Element;
    """
    e1, e2 = nodes
    if e2 is not None:
        e1 = list(e1)
        e1.append(e2)
    return e1


def collect_first_sep(_, nodes):
    """
    Used for:
    Elements = Elements "," Element;
    """
    e1, _, e2 = nodes
    if e2 is not None:
        e1 = list(e1)
        e1.append(e2)
    return e1


def collect_right_first(_, nodes):
    """
    Used for:
    Elements = Element Elements;
    """
    e1, e2 = [nodes[0]], nodes[1]
    e1.extend(e2)
    return e1


def collect_right_first_sep(_, nodes):
    """
    Used for:
    Elements = Element "," Elements;
    """
    e1, e2 = [nodes[0]], nodes[2]
    e1.extend(e2)
    return e1


# Used for productions of the form - one or more elements:
# Elements: Elements Element | Element;
collect = [collect_first, pass_nochange]

# Used for productions of the form - one or more elements:
# Elements: Elements "," Element | Element;
collect_sep = [collect_first_sep, pass_nochange]

# Used for productions of the form - zero or more elements:
# Elements: Elements Element | Element | EMPTY;
collect_optional = [collect_first, pass_nochange, pass_empty]

# Used for productions of the form - zero or more elements:
# Elements: Elements "," Element | Element | EMPTY;
collect_sep_optional = [collect_first_sep, pass_nochange, pass_empty]

# Used for productions of the form - one or more elements:
# Elements: Element Elements | Element;
collect_right = [collect_right_first, pass_nochange]

# Used for productions of the form - one or more elements:
# Elements: Element "," Elements | Element;
collect_right_sep = [collect_right_first_sep, pass_nochange]

# Used for productions of the form - zero or more elements:
# Elements: Element Elements | Element | EMPTY;
collect_right_optional = [collect_right_first, pass_nochange, pass_empty]

# Used for productions of the form - zero or more elements:
# Elements: Element "," Elements | Element | EMPTY;
collect_right_sep_optional = [
    collect_right_first_sep,
    pass_nochange,
    pass_empty,
]

# Used for the production of the form:
# OptionalElement: Element | EMPTY;
optional = [pass_single, pass_none]


def obj(context, nodes, **attrs):
    """
    Creates Python object with the attributes created from named matches.
    This action is used as a default action for rules with named matches.
    """
    cls = context.production.symbol.cls
    instance = cls(**attrs)

    instance._pg_start_position = context.start_position
    instance._pg_end_position = context.end_position

    return instance

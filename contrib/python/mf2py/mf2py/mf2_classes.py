import re

_mf2_classes_re = re.compile("(p|e|u|dt|h)-((:?[a-z0-9]+-)?[a-z]+(:?-[a-z]+)*)$")
_mf2_roots_re = re.compile("h-(:?[a-z0-9]+-)?[a-z]+(:?-[a-z]+)*$")
_mf2_properties_re = re.compile("(p|e|u|dt)-(:?[a-z0-9]+-)?[a-z]+(:?-[a-z]+)*$")
_mf2_e_properties_re = re.compile("e-(:?[a-z0-9]+-)?[a-z]+(:?-[a-z]+)*$")

CONFLICTING_ROOTS_TAILWIND = {"auto", "fit", "full", "max", "min", "px", "screen"}


def filter_classes(classes, regex=_mf2_classes_re):
    """detect classes that are valid names for mf2, sort in dictionary by prefix"""

    types = {x: set() for x in ("u", "p", "dt", "e", "h")}
    for c in classes:
        match = regex.match(c)
        if match:
            if c[0] == "h":
                types["h"].add(c)
            else:
                types[match.group(1)].add(match.group(2))
    return types


def root(classes, filtered_roots):
    return {
        c for c in classes if _mf2_roots_re.match(c) and c[2:] not in filtered_roots
    }


def is_property_class(class_):
    return _mf2_properties_re.match(class_)


def has_embedded_class(classes):
    return any(_mf2_e_properties_re.match(c) for c in classes)

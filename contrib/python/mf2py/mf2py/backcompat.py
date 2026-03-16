"""Looks for classic microformats class names and augments them with
microformats2 names. Ported and adapted from php-mf2.

NOTE: functions in this module modify DOM elements. For this copies of the source tree are created, but BS4's copy
function doesn't create copy of all sub-elements: most notably, the values of the `attrs` dictionary are not copied,
and thus reference the same list objects as in the original tree. Thus, special care has to be taken when modifying the
tree so changes do not accidentally propagate.

a) adding new/removing children is safe

b) attributes (e.g. `class`) should only by changed by assigning a *copy* of the original value, not by modifying it in
place.

DO NOT:
child_classes = child.get('class', [])
child_classes.append('p-kittens')
child['class'] = child_classes

DO:
child_classes = child.get('class', [])[:] ###<------- COPY CREATED HERE
child_classes.append('p-kittens')
child['class'] = child_classes

"""

import copy
import json
import os
from urllib.parse import unquote

import bs4
import library.python.resource

from . import mf2_classes
from .dom_helpers import get_children
from .mf_helpers import unordered_list

# Classic map
_CLASSIC_MAP = {}

# populate backcompat rules from JSON files

_RULES_LOC = 'contrib/python/mf2py/mf2py/backcompat-rules'

for filename in library.python.resource.resfs_files(_RULES_LOC):
    root = os.path.splitext(filename)[0]
    content = library.python.resource.resfs_read(filename)
    rules = json.loads(content.decode())
    _CLASSIC_MAP[root] = rules


def _make_classes_rule(old_classes, new_classes):
    """Builds a rule for augmenting an mf1 class with its mf2
    equivalent(s).
    """

    def f(child, **kwargs):
        child_original = child.original or copy.copy(child)
        child_classes = child.get("class", [])[:]
        if all(cl in child_classes for cl in old_classes):
            child_classes.extend([cl for cl in new_classes if cl not in child_classes])
            child["class"] = child_classes

            # if any new class is e-* attach original to parse originally authored HTML
            if mf2_classes.has_embedded_class(child_classes) and child.original is None:
                child.original = child_original

    return f


def _rel_tag_to_category_rule(child, html_parser, **kwargs):
    """rel=tag converts to p-category using a special transformation (the
    category becomes the tag href's last path segment). This rule adds a new data tag so that
    <a rel="tag" href="http://example.com/tags/cat"></a> gets replaced with
    <data class="p-category" value="cat"></data>
    """

    href = child.get("href", "")
    rels = child.get("rel", [])
    if "tag" in rels and href:
        segments = [seg for seg in href.split("/") if seg]
        if segments:
            if html_parser:
                soup = bs4.BeautifulSoup("", features=html_parser)
            else:
                soup = bs4.BeautifulSoup("")

            data = soup.new_tag("data")
            # this does not use what's given in the JSON
            # but that is not a problem currently
            # use mf1 class so it doesn't get removed later
            data["class"] = ["p-category"]
            data["value"] = unquote(segments[-1])
            child.insert_before(data)
            # remove tag from rels to avoid repeat
            # new list created, original not modifed -> safe on incomplete copy
            child["rel"] = [r for r in rels if r != "tag"]


def _make_rels_rule(old_rels, new_classes, html_parser):
    """Builds a rule for augmenting an mf1 rel with its mf2 class equivalent(s)."""

    # need to special case rel=tag as it operates differently

    def f(child, **kwargs):
        child_rels = child.get("rel", [])
        child_classes = child.get("class", [])[:]
        if all(r in child_rels for r in old_rels):
            if "tag" in old_rels:
                _rel_tag_to_category_rule(child, html_parser, **kwargs)
            else:
                child_classes.extend(
                    [cl for cl in new_classes if cl not in child_classes]
                )
                child["class"] = child_classes

    return f


def _get_rules(old_root, html_parser):
    """for given mf1 root get the rules as a list of functions to act on children"""

    class_rules = [
        _make_classes_rule(old_classes.split(), new_classes)
        for old_classes, new_classes in _CLASSIC_MAP[old_root]
        .get("properties", {})
        .items()
    ]
    rel_rules = [
        _make_rels_rule(old_rels.split(), new_classes, html_parser)
        for old_rels, new_classes in _CLASSIC_MAP[old_root].get("rels", {}).items()
    ]

    return class_rules + rel_rules


def root(classes):
    """get all backcompat root classnames"""
    return unordered_list([c for c in classes if c in _CLASSIC_MAP])


def apply_rules(el, html_parser, filtered_roots):
    """add modern classnames for older mf1 classnames

    returns a copy of el and does not modify the original
    """

    el_copy = copy.copy(el)

    def apply_prop_rules_to_children(parent, rules):
        for child in get_children(parent):
            classes = child.get("class", [])[:]
            # find existing mf2 properties if any and delete them
            child["class"] = [
                cl for cl in classes if not mf2_classes.is_property_class(cl)
            ]

            # apply rules to change mf1 to mf2
            for rule in rules:
                rule(child)

            # recurse if it's not a nested mf1 or mf2 root
            if not (mf2_classes.root(classes, filtered_roots) or root(classes)):
                apply_prop_rules_to_children(child, rules)

    # add mf2 root equivalent
    classes = el_copy.get("class", [])[:]
    old_roots = root(classes)
    for old_root in old_roots:
        new_roots = _CLASSIC_MAP[old_root]["type"]
        classes.extend(new_roots)
    el_copy["class"] = classes

    # add mf2 prop equivalent to descendents and remove existing mf2 props
    rules = []
    for old_root in old_roots:
        rules.extend(_get_rules(old_root, html_parser))

    apply_prop_rules_to_children(el_copy, rules)

    return el_copy

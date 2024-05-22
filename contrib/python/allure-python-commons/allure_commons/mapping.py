from itertools import chain, islice
import attr
import re
from allure_commons.types import Severity, LabelType, LinkType
from allure_commons.types import ALLURE_UNIQUE_LABELS
from allure_commons.model2 import Label, Link


TAG_PREFIX = "allure"

semi_sep = re.compile(r"allure[\.\w]+[^:=]*:")
eq_sep = re.compile(r"allure[\.\w]+[^:=]*=")


def allure_tag_sep(tag):
    if semi_sep.search(tag):
        return ":"
    if eq_sep.search(tag):
        return "="


def __is(kind, t):
    return kind in [v for k, v in t.__dict__.items() if not k.startswith('__')]


def parse_tag(tag, issue_pattern=None, link_pattern=None):
    """
    >>> parse_tag("blocker")
    Label(name='severity', value='blocker')

    >>> parse_tag("allure.issue:http://example.com/BUG-42")
    Link(type='issue', url='http://example.com/BUG-42', name='http://example.com/BUG-42')

    >>> parse_tag("allure.link.home:http://qameta.io")
    Link(type='link', url='http://qameta.io', name='home')

    >>> parse_tag("allure.suite:mapping")
    Label(name='suite', value='mapping')

    >>> parse_tag("allure.suite:mapping")
    Label(name='suite', value='mapping')

    >>> parse_tag("allure.label.owner:me")
    Label(name='owner', value='me')

    >>> parse_tag("foo.label:1")
    Label(name='tag', value='foo.label:1')

    >>> parse_tag("allure.foo:1")
    Label(name='tag', value='allure.foo:1')
    """
    sep = allure_tag_sep(tag)
    schema, value = islice(chain(tag.split(sep, 1), [None]), 2)
    prefix, kind, name = islice(chain(schema.split('.'), [None], [None]), 3)

    if tag in [severity for severity in Severity]:
        return Label(name=LabelType.SEVERITY, value=tag)

    if prefix == TAG_PREFIX and value is not None:

        if __is(kind, LinkType):
            if issue_pattern and kind == "issue" and not value.startswith("http"):
                value = issue_pattern.format(value)
            if link_pattern and kind == "link" and not value.startswith("http"):
                value = link_pattern.format(value)
            return Link(type=kind, name=name or value, url=value)

        if __is(kind, LabelType):
            return Label(name=kind, value=value)

        if kind == "id":
            return Label(name=LabelType.ID, value=value)

        if kind == "label" and name is not None:
            return Label(name=name, value=value)

    return Label(name=LabelType.TAG, value=tag)


def labels_set(labels):
    """
    >>> labels_set([Label(name=LabelType.SEVERITY, value=Severity.NORMAL),
    ...             Label(name=LabelType.SEVERITY, value=Severity.BLOCKER)
    ... ])
    [Label(name='severity', value=<Severity.BLOCKER: 'blocker'>)]

    >>> labels_set([Label(name=LabelType.SEVERITY, value=Severity.NORMAL),
    ...             Label(name='severity', value='minor')
    ... ])
    [Label(name='severity', value='minor')]

    >>> labels_set([Label(name=LabelType.EPIC, value="Epic"),
    ...             Label(name=LabelType.EPIC, value="Epic")
    ... ])
    [Label(name='epic', value='Epic')]

    >>> labels_set([Label(name=LabelType.EPIC, value="Epic1"),
    ...             Label(name=LabelType.EPIC, value="Epic2")
    ... ])
    [Label(name='epic', value='Epic1'), Label(name='epic', value='Epic2')]
    """
    class Wl:
        def __init__(self, label):
            self.label = label

        def __repr__(self):
            return "{name}{value}".format(**attr.asdict(self.label))

        def __eq__(self, other):
            if self.label.name in ALLURE_UNIQUE_LABELS:
                return self.label.name == other.label.name
            return repr(self) == repr(other)

        def __hash__(self):
            if self.label.name in ALLURE_UNIQUE_LABELS:
                return hash(self.label.name)
            return hash(repr(self))

    return sorted([wl.label for wl in set([Wl(label) for label in reversed(labels)])])

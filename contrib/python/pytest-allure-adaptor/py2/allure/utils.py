# encoding: utf-8

'''
This module holds utils for the adaptor

Created on Oct 22, 2013

@author: pupssman
'''

import time
import hashlib
import inspect
import os
import threading
import platform
import socket

from six import text_type, binary_type
from six.moves import filter
from traceback import format_exception_only

from _pytest.python import Module

from allure.constants import Label, Severity


def parents_of(item):
    """
    Returns list of parents (i.e. object.parent values) starting from the top one (Session)
    """
    parents = [item]
    current = item

    while current.parent is not None:
        parents.append(current.parent)
        current = current.parent

    return parents[::-1]


def parent_module(item):
    return next(filter(lambda x: isinstance(x, Module), parents_of(item)))


def parent_down_from_module(item):
    parents = parents_of(item)
    return parents[parents.index(parent_module(item)) + 1:]


def sec2ms(sec):
    return int(round(sec * 1000.0))


def uid(name):
    """
    Generates fancy UID uniquely for ``name`` by the means of hash function
    """
    return hashlib.sha256(name).hexdigest()


def now():
    """
    Return current time in the allure-way representation. No further conversion required.
    """
    return sec2ms(time.time())


def labels_of(item):
    """
    Returns list of TestLabel elements.
    """

    # FIXME: utils should not depend on structure, actually
    from allure.structure import TestLabel

    def get_marker_that_starts_with(item, name):
        """ get a list of marker object from item node that starts with given
        name or empty list if the node doesn't have a marker that starts with
        that name."""
        suitable_names = filter(lambda x: x.startswith(name), item.keywords.keys())

        markers = list()
        for suitable_name in suitable_names:
            for marker in item.iter_markers(name=suitable_name):
                markers.append(marker)

        return markers

    labels = []
    label_markers = get_marker_that_starts_with(item, Label.DEFAULT)
    for label_marker in label_markers:
        label_name = label_marker.name.split('.', 1)[-1]
        for label_value in label_marker.args or ():
            labels.append(TestLabel(name=label_name, value=label_value))

    if not any(l.name == Label.SEVERITY for l in labels):
        labels.append(TestLabel(name=Label.SEVERITY, value=Severity.NORMAL))

    labels.append(TestLabel(name=Label.THREAD, value=thread_tag()))
    labels.append(TestLabel(name=Label.HOST, value=host_tag()))
    labels.append(TestLabel(name=Label.FRAMEWORK, value='pytest'))
    labels.append(TestLabel(name=Label.LANGUAGE, value=platform_tag()))

    return labels


def all_of(enum):
    """
    returns list of name-value pairs for ``enum`` from :py:mod:`allure.constants`
    """

    def clear_pairs(pair):
        if pair[0].startswith('_'):
            return False
        if pair[0] in ('name', 'value'):
            return False
        return True

    return filter(clear_pairs, inspect.getmembers(enum))


def unicodify(something):
    if isinstance(something, text_type):
        return something
    elif isinstance(something, binary_type):
        return something.decode('utf-8', 'replace')
    else:
        try:
            return text_type(something)  # @UndefinedVariable
        except (UnicodeEncodeError, UnicodeDecodeError):
            return u'<nonpresentable %s>' % type(something)  # @UndefinedVariable


def present_exception(e):
    """
    Try our best at presenting the exception in a readable form
    """
    if not isinstance(e, SyntaxError):
        return unicodify('%s: %s' % (type(e).__name__, unicodify(e)))
    else:
        return unicodify(format_exception_only(SyntaxError, e))


def get_exception_message(excinfo, pyteststatus, report):
    """
    :param excinfo: a :py:class:`py._code.code.ExceptionInfo` from call.excinfo
    :param pyteststatus: the failed/xfailed/xpassed thing
    get exception message from pytest's internal ``report`` object
    """
    return (excinfo and present_exception(excinfo.value)) or \
           (hasattr(report, "wasxfail") and report.skipped and "xfailed") or \
           (hasattr(report, "wasxfail") and report.failed and "xpassed") or \
           (pyteststatus) or report.outcome


def thread_tag():
    """
    Return a special build_tag value, consists of PID and thread_name.
    """
    return '{0}-{1}'.format(os.getpid(), threading.current_thread().name)


def host_tag():
    """
    Return a special host_tag value, representing current host.
    """
    return socket.gethostname()


def platform_tag():
    """
    Return a special platform_tag value represent python type and version
    """
    major_version, _, __ = platform.python_version_tuple()
    implementation = platform.python_implementation()
    return '{implementation}{major_version}'.format(implementation=implementation.lower(),
                                                    major_version=major_version)


def mangle_testnames(names):
    names = [x.replace(".py", "") for x in names if x != '()']
    names[0] = names[0].replace("/", '.')
    return names


def marker_of_node(node, name):
    """
    Return marker of node by name
    """
    try:
        return node.get_marker(name)
    except AttributeError:
        return node.get_closest_marker(name)

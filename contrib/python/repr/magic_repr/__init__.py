# coding: utf-8

import six
import sys
import threading

from six.moves import zip
from itertools import starmap, chain
from operator import attrgetter


__version__ = "0.3.1"

__all__ = ['make_repr']


ON_PYTHON2 = sys.version_info.major == 2


def force_unicode(value):
    """If input string is binary, then decode from utf-8."""
    if isinstance(value, six.binary_type):
        return value.decode('utf-8')
    return value


def is_multiline(text):
    return u'\n' in text


def padding_adder(padding):

    def adder(text, ignore_first_line=False):
        pad_text = u' ' * padding
        lines = text.split(u'\n')
        first_line, rest = cut_head(lines)

        # check if user wants to not add padding
        # for the first line
        if not ignore_first_line:
            first_line = pad_text + first_line

        rest = (pad_text + line
                for line in rest)

        lines = chain((first_line, ), rest)
        return u'\n'.join(lines)

    return adder


def cut_head(lst):
    """Returns first element and an iterator over rest element.

    TODO: will be useful to make it work not only with lists
          but also with any iterable.
    """
    assert len(lst) > 0, 'Argument lst should have at least one item'
    return lst[0], lst[1:]


def serialize_text(out, text):
    """This method is used to append content of the `text`
    argument to the `out` argument.

    Depending on how many lines in the text, a
    padding can be added to all lines except the first
    one.

    Concatenation result is appended to the `out` argument.
    """
    padding = len(out)
    # we need to add padding to all lines
    # except the first one
    add_padding = padding_adder(padding)
    text = add_padding(text, ignore_first_line=True)

    return out + text


def serialize_list(out, lst, delimiter=u'', max_length=20):

    """This method is used to serialize list of text
    pieces like ["some=u'Another'", "blah=124"]

    Depending on how many lines are in these items,
    they are concatenated in row or as a column.

    Concatenation result is appended to the `out` argument.
    """

    have_multiline_items = any(map(is_multiline, lst))
    result_will_be_too_long = sum(map(len, lst)) > max_length

    if have_multiline_items or result_will_be_too_long:
        padding = len(out)
        add_padding = padding_adder(padding)

        # we need to add padding to all lines
        # except the first one
        head, rest = cut_head(lst)
        rest = map(add_padding, rest)

        # add padding to the head, but not for it's first line
        head = add_padding(head, ignore_first_line=True)

        # now join lines back
        lst = chain((head,), rest)
        delimiter += u'\n'
    else:
        delimiter += u' '

    return out + delimiter.join(lst)


class RecursionBreaker(threading.local):
    def __init__(self):
        super(RecursionBreaker, self).__init__()
        self.processed = set()


recursion_breaker = RecursionBreaker()


def format_value(value):
    """This function should return unicode representation of the value
    """
    value_id = id(value)

    if value_id in recursion_breaker.processed:
        return u'<recursion>'

    recursion_breaker.processed.add(value_id)

    try:
        if isinstance(value, six.binary_type):
            # suppose, all byte strings are in unicode
            # don't know if everybody in the world uses anything else?
            return u"'{0}'".format(value.decode('utf-8'))

        elif isinstance(value, six.text_type):
            return u"u'{0}'".format(value)

        elif isinstance(value, (list, tuple)):
            # long lists or lists with multiline items
            # will be shown vertically
            values = list(map(format_value, value))
            result = serialize_list(u'[', values, delimiter=u',') + u']'
            return force_unicode(result)

        elif isinstance(value, dict):
            items = six.iteritems(value)

            # format each key/value pair as a text,
            # calling format_value recursively
            items = (tuple(map(format_value, item))
                     for item in items)

            items = list(items)
            # sort by keys for readability
            items.sort()

            # for each item value
            items = [
                serialize_text(
                    u'{0}: '.format(key),
                    item_value)
                for key, item_value in items]

            # and serialize these pieces as a list, enclosing
            # them into a curve brackets
            result = serialize_list(u'{', items, delimiter=u',') + u'}'
            return force_unicode(result)
        return force_unicode(repr(value))

    finally:
        recursion_breaker.processed.remove(value_id)


def make_repr(*args, **kwargs):
    """Returns __repr__ method which returns ASCII
    representaion of the object with given fields.

    Without arguments, ``make_repr`` generates a method
    which outputs all object's non-protected (non-undercored)
    arguments which are not callables.

    Accepts ``*args``, which should be a names of object's
    attributes to be included in the output::

      __repr__ = make_repr('foo', 'bar')

    If you want to generate attribute's content on the fly,
    then you should use keyword arguments and pass a callable
    of one argument::

      __repr__ = make_repr(foo=lambda obj: obj.blah + 100500)

    """

    def method(self):
        cls_name = self.__class__.__name__

        if args:
            field_names = args
        else:
            def undercored(name): return name.startswith('_')

            def is_method(name): return callable(getattr(self, name))

            def good_name(name):
                return not undercored(name) and not is_method(name)

            field_names = filter(good_name, dir(self))
            field_names = sorted(field_names)

        # on this stage, we make from field_names an
        # attribute getters
        field_getters = zip(field_names,
                            map(attrgetter, field_names))

        # now process keyword args, they must
        # contain callables of one argument
        # and callable should return a field's value
        field_getters = chain(
            field_getters,
            kwargs.items())

        fields = ((name, format_value(getter(self)))
                  for name, getter in field_getters)

        # prepare key strings
        fields = ((u'{0}='.format(name), value)
                  for name, value in fields)

        # join values with they respective keys
        fields = list(starmap(serialize_text, fields))

        beginning = u'<{cls_name} '.format(
            cls_name=cls_name,
        )
        result = serialize_list(
            beginning,
            fields)

        # append closing braket
        result += u'>'

        if ON_PYTHON2:
            # on python 2.x repr returns bytes, but on python3 - unicode strings
            result = result.encode('utf-8')

        return result

    return method

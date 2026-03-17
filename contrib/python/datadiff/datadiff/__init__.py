"""
Copyright 2012, 2014 Dave Brondsema

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
from difflib import SequenceMatcher, unified_diff
import sys
from numbers import Number

__version_info__ = 2, 2, 0
__version__ = '.'.join(map(str, __version_info__))

log = logging.getLogger('datadiff')

PY2 = sys.version_info < (3,)

string_types = str,
if PY2:
    string_types = eval('basestring'),

"""
For each type, we need:
* a diff()
* start/end strings
* conversion to hashable
"""


class NotHashable(TypeError):
    pass


class NotSequence(TypeError):
    pass


class DiffTypeError(TypeError):
    pass


class DiffNotImplementedForType(DiffTypeError):
    def __init__(self, attempted_type):
        self.attempted_type = attempted_type

    def __str__(self):
        return "diff() not implemented for %s" % self.attempted_type


def unified_diff_strings(a, b, fromfile='', tofile='', fromfiledate='', tofiledate='', context=3):
    """
    Wrapper around difflib.unified_diff that accepts 'a' and 'b' as multi-line strings
    and returns a multi-line string, instead of lists of strings.
    """
    return '\n'.join(unified_diff(a.split('\n'), b.split('\n'),
                                  fromfile, tofile, fromfiledate, tofiledate, context,
                                  lineterm=''))


def diff(a, b, context=3, depth=0, fromfile='a', tofile='b', places=None):
    if isinstance(a, string_types) and isinstance(b, string_types):
        # special cases
        if '\n' in a or '\n' in b:
            return unified_diff_strings(a, b, fromfile=fromfile, tofile=tofile, context=context)
        else:
            # even though technically it is a sequence,
            # we don't want to diff char-by-char
            raise DiffNotImplementedForType(str)
    if type(a) != type(b):
        raise DiffTypeError(
            'Types differ: %s=%s %s=%s  Values of a and b are: %r, %r' % (fromfile, tofile, type(a), type(b), a, b))
    if type(a) == dict:
        return diff_dict(a, b, context, depth, fromfile=fromfile, tofile=tofile, places=places)
    if hasattr(a, 'intersection') and hasattr(a, 'difference'):
        return diff_set(a, b, context, depth, fromfile=fromfile, tofile=tofile, places=places)
    try:
        return try_diff_seq(a, b, context, depth, fromfile=fromfile, tofile=tofile, places=places)
    except NotSequence:
        raise DiffNotImplementedForType(type(a))


class DataDiff(object):

    def __init__(self, datatype, type_start_str=None, type_end_str=None, fromfile='a', tofile='b'):
        self.diffs = []
        self.datatype = datatype
        self.fromfile = fromfile
        self.tofile = tofile
        if type_end_str is None:
            if type_start_str is not None:
                raise Exception("Must specify both type_start_str and type_end_str, or neither")
            self.type_start_str = datatype.__name__ + '(['
            self.type_end_str = '])'
        else:
            self.type_start_str = type_start_str
            self.type_end_str = type_end_str

    def context(self, a_start, a_end, b_start, b_end):
        self.diffs.append(('context', [a_start, a_end, b_start, b_end]))

    def context_end_container(self):
        self.diffs.append(('context_end_container', []))

    def nested(self, datadiff):
        self.diffs.append(('datadiff', datadiff))

    def multi(self, change, items):
        if items:
            self.diffs.append((change, items))

    def delete(self, item):
        return self.multi('delete', [item])

    def insert(self, item):
        return self.multi('insert', [item])

    def equal(self, item):
        return self.multi('equal', [item])

    def insert_multi(self, items):
        return self.multi('insert', items)

    def delete_multi(self, items):
        return self.multi('delete', items)

    def equal_multi(self, items):
        return self.multi('equal', items)

    def __str__(self):
        return self.stringify()

    def stringify(self, depth=0, include_preamble=True):
        if not self.diffs:
            return ''
        output = []
        if depth == 0 and include_preamble:
            output.append('--- %s' % self.fromfile)
            output.append('+++ %s' % self.tofile)
        output.append(' ' * depth + self.type_start_str)
        for change, items in self.diffs:
            if change == 'context':
                context_a = str(items[0])
                if items[0] != items[1]:
                    context_a += ',' + str(items[1])
                context_b = str(items[2])
                if items[2] != items[3]:
                    context_b += ',' + str(items[3])
                output.append(' ' * depth + '@@ -%s +%s @@' % (context_a, context_b))
                continue
            if change == 'context_end_container':
                output.append(' ' * depth + '@@  @@')
                continue
            elif change == 'datadiff':
                output.append(' ' * depth + items.stringify(depth + 1) + ',')
                continue
            if change == 'delete':
                ch = '-'
            elif change == 'insert':
                ch = '+'
            elif change == 'equal':
                ch = ' '
            else:
                raise Exception('Unknown change type %r' % change)
            for item in items:
                output.append(' ' * depth + "%s%r," % (ch, item))
        output.append(' ' * depth + self.type_end_str)
        return '\n'.join(output)

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return bool([d for d in self.diffs if d[0] != 'equal'])


def hashable(s, places=None):
    try:
        # convert top-level container
        if type(s) == list:
            ret = tuple(s)
        elif type(s) == dict:
            ret = frozenset(hashable(_, places=places) for _ in s.items())
        elif type(s) == set:
            ret = frozenset(s)
        elif type(s) == float and places is not None:
            ret = round(s, places)
        else:
            ret = s

        # make it recursive
        if type(ret) == tuple:
            ret = tuple(hashable(_, places=places) for _ in ret)

        # validate
        hash(ret)
    except TypeError:
        log.debug('hashable error', exc_info=True)
        raise NotHashable("Hashable type required (for parent diff) but got %s with value %r" % (type(s), s))
    else:
        return ret


def try_diff_seq(a, b, context=3, depth=0, fromfile='a', tofile='b', places=None):
    """
    Safe to try any containers with this function, to see if it might be a sequence
    Raises TypeError if its not a sequence
    """
    try:
        return diff_seq(a, b, context, depth, fromfile=fromfile, tofile=tofile, places=places)
    except NotHashable:
        raise
    except Exception:
        log.debug('tried SequenceMatcher but got error', exc_info=True)
        raise NotSequence("Cannot use SequenceMatcher on %s" % type(a))


def diff_seq(a, b, context=3, depth=0, fromfile='a', tofile='b', places=None):
    if not hasattr(a, '__iter__') and not hasattr(a, '__getitem__'):
        raise NotSequence("Not a sequence %s" % type(a))
    hashable_a = [hashable(_, places=places) for _ in a]
    hashable_b = [hashable(_, places=places) for _ in b]
    sm = SequenceMatcher(a=hashable_a, b=hashable_b)
    if type(a) == tuple:
        ddiff = DataDiff(tuple, '(', ')', fromfile=fromfile, tofile=tofile)
    elif type(b) == list:
        ddiff = DataDiff(list, '[', ']', fromfile=fromfile, tofile=tofile)
    else:
        ddiff = DataDiff(type(a), fromfile=fromfile, tofile=tofile)
    for chunk in sm.get_grouped_opcodes(context):
        ddiff.context(max(chunk[0][1] - 1, 0), max(chunk[-1][2] - 1, 0),
                      max(chunk[0][3] - 1, 0), max(chunk[-1][4] - 1, 0))
        for change, i1, i2, j1, j2 in chunk:
            if change == 'replace':
                consecutive_deletes = []
                consecutive_inserts = []
                for a2, b2 in zip(a[i1:i2], b[j1:j2]):
                    try:
                        nested_diff = diff(a2, b2, context, depth + 1)
                        ddiff.delete_multi(consecutive_deletes)
                        ddiff.insert_multi(consecutive_inserts)
                        consecutive_deletes = []
                        consecutive_inserts = []
                        ddiff.nested(nested_diff)
                    except DiffTypeError:
                        consecutive_deletes.append(a2)
                        consecutive_inserts.append(b2)

                # differing lengths get truncated by zip()
                # here we handle the truncated items
                ddiff.delete_multi(consecutive_deletes)
                if i2 - i1 > j2 - j1:
                    common_length = j2 - j1  # covered by zip
                    ddiff.delete_multi(a[i1 + common_length:i2])
                ddiff.insert_multi(consecutive_inserts)
                if i2 - i1 < j2 - j1:
                    common_length = i2 - i1  # covered by zip
                    ddiff.insert_multi(b[j1 + common_length:j2])
            else:
                if change == 'insert':
                    items = b[j1:j2]
                else:
                    items = a[i1:i2]
                ddiff.multi(change, items)
        if i2 < len(a):
            ddiff.context_end_container()
    return ddiff


class dictitem(tuple):
    def __repr__(self):
        key, val = self
        if type(val) == DataDiff:
            diff_val = val.stringify(depth=self.depth, include_preamble=False)
            return "%r: %s" % (key, diff_val.strip())
        return "%r: %r" % (key, val)


def diff_dict(a, b, context=3, depth=0, fromfile='a', tofile='b', places=None):
    ddiff = DataDiff(dict, '{', '}', fromfile=fromfile, tofile=tofile)
    for key in a.keys():
        if key not in b:
            ddiff.delete(dictitem((key, a[key])))
        elif places is not None and type(a[key]) == type(b[key]) == float \
                and round(a[key], places) == round(b[key], places):
            pass
        elif a[key] != b[key]:
            try:
                nested_diff = diff(a[key], b[key], context, depth + 1, places=places)
                nested_item = dictitem((key, nested_diff))
                nested_item.depth = depth + 1
                ddiff.equal(nested_item)  # not really equal
            except DiffTypeError:
                ddiff.delete(dictitem((key, a[key])))
                ddiff.insert(dictitem((key, b[key])))
        else:
            if context:
                ddiff.equal(dictitem((key, a[key])))
            context -= 1
    for key in b:
        if key not in a:
            ddiff.insert(dictitem((key, b[key])))

    def diffitem_dictitem_sort_key(diffitem):
        change, dictitem = diffitem
        key = dictitem[0][0]
        # use hash, to make sure its always orderable against other potential key types
        if isinstance(key, string_types) or isinstance(key, Number):
            return key
        else:
            return abs(hash(key))  # abs for consistency between py2/3, at least for datetime

    ddiff.diffs.sort(key=diffitem_dictitem_sort_key)

    if ddiff and context < 0:
        ddiff.context_end_container()

    return ddiff


def diff_set(a, b, context=3, depth=0, fromfile='b', tofile='a', places=None):
    norm_a = set(map(lambda x: round(x, places), a)) if places is not None else a
    norm_b = set(map(lambda x: round(x, places), b)) if places is not None else b
    ddiff = DataDiff(type(a), fromfile=fromfile, tofile=tofile)
    ddiff.delete_multi(norm_a - norm_b)
    ddiff.insert_multi(norm_b - norm_a)
    equal = list(norm_a.intersection(norm_b))
    ddiff.equal_multi(equal[:context])
    if len(equal) > context:
        ddiff.context_end_container()
    return ddiff

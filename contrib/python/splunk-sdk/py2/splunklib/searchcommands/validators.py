# coding=utf-8
#
# Copyright 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals

from json.encoder import encode_basestring_ascii as json_encode_string
from collections import namedtuple
from splunklib.six.moves import StringIO
from io import open
import csv
import os
import re
from splunklib import six
from splunklib.six.moves import getcwd


class Validator(object):
    """ Base class for validators that check and format search command options.

    You must inherit from this class and override :code:`Validator.__call__` and
    :code:`Validator.format`. :code:`Validator.__call__` should convert the
    value it receives as argument and then return it or raise a
    :code:`ValueError`, if the value will not convert.

    :code:`Validator.format` should return a human readable version of the value
    it receives as argument the same way :code:`str` does.

    """
    def __call__(self, value):
        raise NotImplementedError()

    def format(self, value):
        raise NotImplementedError()


class Boolean(Validator):
    """ Validates Boolean option values.

    """
    truth_values = {
        '1': True, '0': False,
        't': True, 'f': False,
        'true': True, 'false': False,
        'y': True, 'n': False,
        'yes': True, 'no': False
    }

    def __call__(self, value):
        if not (value is None or isinstance(value, bool)):
            value = six.text_type(value).lower()
            if value not in Boolean.truth_values:
                raise ValueError('Unrecognized truth value: {0}'.format(value))
            value = Boolean.truth_values[value]
        return value

    def format(self, value):
        return None if value is None else 't' if value else 'f'


class Code(Validator):
    """ Validates code option values.

    This validator compiles an option value into a Python code object that can be executed by :func:`exec` or evaluated
    by :func:`eval`. The value returned is a :func:`namedtuple` with two members: object, the result of compilation, and
    source, the original option value.

    """
    def __init__(self, mode='eval'):
        """
        :param mode: Specifies what kind of code must be compiled; it can be :const:`'exec'`, if source consists of a
            sequence of statements, :const:`'eval'`, if it consists of a single expression, or :const:`'single'` if it
            consists of a single interactive statement. In the latter case, expression statements that evaluate to
            something other than :const:`None` will be printed.
        :type mode: unicode or bytes

        """
        self._mode = mode

    def __call__(self, value):
        if value is None:
            return None
        try:
            return Code.object(compile(value, 'string', self._mode), six.text_type(value))
        except (SyntaxError, TypeError) as error:
            message = str(error)

            six.raise_from(ValueError(message), error)

    def format(self, value):
        return None if value is None else value.source

    object = namedtuple('Code', ('object', 'source'))


class Fieldname(Validator):
    """ Validates field name option values.

    """
    pattern = re.compile(r'''[_.a-zA-Z-][_.a-zA-Z0-9-]*$''')

    def __call__(self, value):
        if value is not None:
            value = six.text_type(value)
            if Fieldname.pattern.match(value) is None:
                raise ValueError('Illegal characters in fieldname: {}'.format(value))
        return value

    def format(self, value):
        return value


class File(Validator):
    """ Validates file option values.

    """
    def __init__(self, mode='rt', buffering=None, directory=None):
        self.mode = mode
        self.buffering = buffering
        self.directory = File._var_run_splunk if directory is None else directory

    def __call__(self, value):

        if value is None:
            return value

        path = six.text_type(value)

        if not os.path.isabs(path):
            path = os.path.join(self.directory, path)

        try:
            value = open(path, self.mode) if self.buffering is None else open(path, self.mode, self.buffering)
        except IOError as error:
            raise ValueError('Cannot open {0} with mode={1} and buffering={2}: {3}'.format(
                value, self.mode, self.buffering, error))

        return value

    def format(self, value):
        return None if value is None else value.name

    _var_run_splunk = os.path.join(
        os.environ['SPLUNK_HOME'] if 'SPLUNK_HOME' in os.environ else getcwd(), 'var', 'run', 'splunk')


class Integer(Validator):
    """ Validates integer option values.

    """
    def __init__(self, minimum=None, maximum=None):
        if minimum is not None and maximum is not None:
            def check_range(value):
                if not (minimum <= value <= maximum):
                    raise ValueError('Expected integer in the range [{0},{1}], not {2}'.format(minimum, maximum, value))
                return
        elif minimum is not None:
            def check_range(value):
                if value < minimum:
                    raise ValueError('Expected integer in the range [{0},+∞], not {1}'.format(minimum, value))
                return
        elif maximum is not None:
            def check_range(value):
                if value > maximum:
                    raise ValueError('Expected integer in the range [-∞,{0}], not {1}'.format(maximum, value))
                return
        else:
            def check_range(value):
                return

        self.check_range = check_range
        return

    def __call__(self, value):
        if value is None:
            return None
        try:
            if six.PY2:
                value = long(value)
            else:
                value = int(value)
        except ValueError:
            raise ValueError('Expected integer value, not {}'.format(json_encode_string(value)))

        self.check_range(value)
        return value

    def format(self, value):
        return None if value is None else six.text_type(int(value))


class Float(Validator):
    """ Validates float option values.

    """
    def __init__(self, minimum=None, maximum=None):
        if minimum is not None and maximum is not None:
            def check_range(value):
                if not (minimum <= value <= maximum):
                    raise ValueError('Expected float in the range [{0},{1}], not {2}'.format(minimum, maximum, value))
                return
        elif minimum is not None:
            def check_range(value):
                if value < minimum:
                    raise ValueError('Expected float in the range [{0},+∞], not {1}'.format(minimum, value))
                return
        elif maximum is not None:
            def check_range(value):
                if value > maximum:
                    raise ValueError('Expected float in the range [-∞,{0}], not {1}'.format(maximum, value))
                return
        else:
            def check_range(value):
                return

        self.check_range = check_range
        return

    def __call__(self, value):
        if value is None:
            return None
        try:
            value = float(value)
        except ValueError:
            raise ValueError('Expected float value, not {}'.format(json_encode_string(value)))

        self.check_range(value)
        return value

    def format(self, value):
        return None if value is None else six.text_type(float(value))


class Duration(Validator):
    """ Validates duration option values.

    """
    def __call__(self, value):

        if value is None:
            return None

        p = value.split(':', 2)
        result = None
        _60 = Duration._60
        _unsigned = Duration._unsigned

        try:
            if len(p) == 1:
                result = _unsigned(p[0])
            if len(p) == 2:
                result = 60 * _unsigned(p[0]) + _60(p[1])
            if len(p) == 3:
                result = 3600 * _unsigned(p[0]) + 60 * _60(p[1]) + _60(p[2])
        except ValueError:
            raise ValueError('Invalid duration value: {0}'.format(value))

        return result

    def format(self, value):

        if value is None:
            return None

        value = int(value)

        s = value % 60
        m = value // 60 % 60
        h = value // (60 * 60)

        return '{0:02d}:{1:02d}:{2:02d}'.format(h, m, s)

    _60 = Integer(0, 59)
    _unsigned = Integer(0)


class List(Validator):
    """ Validates a list of strings

    """
    class Dialect(csv.Dialect):
        """ Describes the properties of list option values. """
        strict = True
        delimiter = str(',')
        quotechar = str('"')
        doublequote = True
        lineterminator = str('\n')
        skipinitialspace = True
        quoting = csv.QUOTE_MINIMAL

    def __init__(self, validator=None):
        if not (validator is None or isinstance(validator, Validator)):
            raise ValueError('Expected a Validator instance or None for validator, not {}', repr(validator))
        self._validator = validator

    def __call__(self, value):

        if value is None or isinstance(value, list):
            return value

        try:
            value = next(csv.reader([value], self.Dialect))
        except csv.Error as error:
            raise ValueError(error)

        if self._validator is None:
            return value

        try:
            for index, item in enumerate(value):
                value[index] = self._validator(item)
        except ValueError as error:
            raise ValueError('Could not convert item {}: {}'.format(index, error))

        return value

    def format(self, value):
        output = StringIO()
        writer = csv.writer(output, List.Dialect)
        writer.writerow(value)
        value = output.getvalue()
        return value[:-1]


class Map(Validator):
    """ Validates map option values.

    """
    def __init__(self, **kwargs):
        self.membership = kwargs

    def __call__(self, value):

        if value is None:
            return None

        value = six.text_type(value)

        if value not in self.membership:
            raise ValueError('Unrecognized value: {0}'.format(value))

        return self.membership[value]

    def format(self, value):
        return None if value is None else list(self.membership.keys())[list(self.membership.values()).index(value)]


class Match(Validator):
    """ Validates that a value matches a regular expression pattern.

    """
    def __init__(self, name, pattern, flags=0):
        self.name = six.text_type(name)
        self.pattern = re.compile(pattern, flags)

    def __call__(self, value):
        if value is None:
            return None
        value = six.text_type(value)
        if self.pattern.match(value) is None:
            raise ValueError('Expected {}, not {}'.format(self.name, json_encode_string(value)))
        return value

    def format(self, value):
        return None if value is None else six.text_type(value)


class OptionName(Validator):
    """ Validates option names.

    """
    pattern = re.compile(r'''(?=\w)[^\d]\w*$''', re.UNICODE)

    def __call__(self, value):
        if value is not None:
            value = six.text_type(value)
            if OptionName.pattern.match(value) is None:
                raise ValueError('Illegal characters in option name: {}'.format(value))
        return value

    def format(self, value):
        return None if value is None else six.text_type(value)


class RegularExpression(Validator):
    """ Validates regular expression option values.

    """
    def __call__(self, value):
        if value is None:
            return None
        try:
            value = re.compile(six.text_type(value))
        except re.error as error:
            raise ValueError('{}: {}'.format(six.text_type(error).capitalize(), value))
        return value

    def format(self, value):
        return None if value is None else value.pattern


class Set(Validator):
    """ Validates set option values.

    """
    def __init__(self, *args):
        self.membership = set(args)

    def __call__(self, value):
        if value is None:
            return None
        value = six.text_type(value)
        if value not in self.membership:
            raise ValueError('Unrecognized value: {}'.format(value))
        return value

    def format(self, value):
        return self.__call__(value)


__all__ = ['Boolean', 'Code', 'Duration', 'File', 'Integer', 'Float', 'List', 'Map', 'RegularExpression', 'Set']

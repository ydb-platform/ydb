"""Step parsers."""

from __future__ import absolute_import

import re as base_re

import parse as base_parse
import six
from parse_type import cfparse as base_cfparse

from .exceptions import InvalidStepParserError


class StepParser(object):
    """Parser of the individual step."""

    def __init__(self, name):
        self.name = name

    def parse_arguments(self, name):
        """Get step arguments from the given step name.

        :return: `dict` of step arguments
        """
        raise NotImplementedError()

    def is_matching(self, name):
        """Match given name with the step name."""
        raise NotImplementedError()


class re(StepParser):
    """Regex step parser."""

    def __init__(self, name, *args, **kwargs):
        """Compile regex."""
        super(re, self).__init__(name)
        self.regex = base_re.compile(self.name, *args, **kwargs)

    def parse_arguments(self, name):
        """Get step arguments.

        :return: `dict` of step arguments
        """
        return self.regex.match(name).groupdict()

    def is_matching(self, name):
        """Match given name with the step name."""
        return bool(self.regex.match(name))


class parse(StepParser):
    """parse step parser."""

    def __init__(self, name, *args, **kwargs):
        """Compile parse expression."""
        super(parse, self).__init__(name)
        self.parser = base_parse.compile(self.name, *args, **kwargs)

    def parse_arguments(self, name):
        """Get step arguments.

        :return: `dict` of step arguments
        """
        return self.parser.parse(name).named

    def is_matching(self, name):
        """Match given name with the step name."""
        try:
            return bool(self.parser.parse(name))
        except ValueError:
            return False


class cfparse(parse):
    """cfparse step parser."""

    def __init__(self, name, *args, **kwargs):
        """Compile parse expression."""
        super(parse, self).__init__(name)
        self.parser = base_cfparse.Parser(self.name, *args, **kwargs)


class string(StepParser):
    """Exact string step parser."""

    def parse_arguments(self, name):
        """No parameters are available for simple string step.

        :return: `dict` of step arguments
        """
        return {}

    def is_matching(self, name):
        """Match given name with the step name."""
        return self.name == name


def get_parser(step_name):
    """Get parser by given name.

    :param step_name: name of the step to parse

    :return: step parser object
    :rtype: StepArgumentParser
    """
    if isinstance(step_name, six.string_types):
        if isinstance(step_name, six.binary_type):  # Python 2 compatibility
            step_name = step_name.decode("utf-8")
        return string(step_name)
    elif not hasattr(step_name, "is_matching") or not hasattr(step_name, "parse_arguments"):
        raise InvalidStepParserError(step_name)
    else:
        return step_name

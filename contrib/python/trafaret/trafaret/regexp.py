import re
from .base import Trafaret, String
from .lib import STR_TYPES
from . import codes


class RegexpRaw(Trafaret):
    """
    Check if given string match given regexp
    """
    __slots__ = ('regexp', 'raw_regexp')

    def __init__(self, regexp, re_flags=0):
        self.regexp = re.compile(regexp, re_flags) if isinstance(regexp, STR_TYPES) else regexp
        self.raw_regexp = self.regexp.pattern if self.regexp else None

    def check_and_return(self, value):
        if not isinstance(value, STR_TYPES):
            self._failure("value is not a string", value=value, code=codes.IS_NOT_A_STRING)
        match = self.regexp.match(value)
        if not match:
            self._failure('does not match pattern %s' % self.raw_regexp, value=value, code=codes.DOES_NOT_MATCH_RE)
        return match

    def __repr__(self):
        return '<Regexp "%s">' % self.raw_regexp


class Regexp(RegexpRaw):
    def check_and_return(self, value):
        return super(Regexp, self).check_and_return(value).group()


class RegexpString(String, Regexp):
    __slots__ = ()
    # regex: str
    str_method = None

    def __init__(self, *args, **kwargs):
        String.__init__(self, *args, **kwargs)
        Regexp.__init__(self, self.regex)

    def check_and_return(self, value):
        str_value = String.check_and_return(self, value)
        if self.str_method is not None:
            str_value = getattr(str_value, self.str_method)()
        return Regexp.check_and_return(self, str_value)

    def __repr__(self):
        return '<RegexpString "%s">' % self.regex

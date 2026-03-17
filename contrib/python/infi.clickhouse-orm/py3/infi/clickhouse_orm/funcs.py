from functools import wraps
from inspect import signature, Parameter
from types import FunctionType

from .utils import is_iterable, comma_join, NO_VALUE, arg_to_sql
from .query import Cond, QuerySet


def binary_operator(func):
    """
    Decorates a function to mark it as a binary operator.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        ret = func(*args, **kwargs)
        ret.is_binary_operator = True
        return ret
    return wrapper


def type_conversion(func):
    """
    Decorates a function to mark it as a type conversion function.
    The metaclass automatically generates "OrZero" and "OrNull" combinators
    for the decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    wrapper.f_type = 'type_conversion'
    return wrapper


def aggregate(func):
    """
    Decorates a function to mark it as an aggregate function.
    The metaclass automatically generates combinators such as "OrDefault",
    "OrNull", "If" etc. for the decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    wrapper.f_type = 'aggregate'
    return wrapper


def with_utf8_support(func):
    """
    Decorates a function to mark it as a string function that has a UTF8 variant.
    The metaclass automatically generates a "UTF8" combinator for the decorated function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    wrapper.f_type = 'with_utf8_support'
    return wrapper


def parametric(func):
    """
    Decorates a function to convert it to a parametric function, such
    as `quantile(level)(expr)`.
    """
    @wraps(func)
    def wrapper(*parameters):
        @wraps(func)
        def inner(*args, **kwargs):
            f = func(*args, **kwargs)
            # Append the parameter to the function name
            parameters_str = comma_join(parameters, stringify=True)
            f.name = '%s(%s)' % (f.name, parameters_str)
            return f
        return inner
    wrapper.f_parametric = True
    return wrapper


class FunctionOperatorsMixin(object):
    """
    A mixin for implementing Python operators using F objects.
    """

    # Comparison operators

    def __lt__(self, other):
        return F.less(self, other)

    def __le__(self, other):
        return F.lessOrEquals(self, other)

    def __eq__(self, other):
        return F.equals(self, other)

    def __ne__(self, other):
        return F.notEquals(self, other)

    def __gt__(self, other):
        return F.greater(self, other)

    def __ge__(self, other):
        return F.greaterOrEquals(self, other)

    # Arithmetic operators

    def __add__(self, other):
        return F.plus(self, other)

    def __radd__(self, other):
        return F.plus(other, self)

    def __sub__(self, other):
        return F.minus(self, other)

    def __rsub__(self, other):
        return F.minus(other, self)

    def __mul__(self, other):
        return F.multiply(self, other)

    def __rmul__(self, other):
        return F.multiply(other, self)

    def __truediv__(self, other):
        return F.divide(self, other)

    def __rtruediv__(self, other):
        return F.divide(other, self)

    def __floordiv__(self, other):
        return F.intDiv(self, other)

    def __rfloordiv__(self, other):
        return F.intDiv(other, self)

    def __mod__(self, other):
        return F.modulo(self, other)

    def __rmod__(self, other):
        return F.modulo(other, self)

    def __neg__(self):
        return F.negate(self)

    def __pos__(self):
        return self

    # Logical operators

    def __and__(self, other):
        return F._and(self, other)

    def __rand__(self, other):
        return F._and(other, self)

    def __or__(self, other):
        return F._or(self, other)

    def __ror__(self, other):
        return F._or(other, self)

    def __xor__(self, other):
        return F._xor(self, other)

    def __rxor__(self, other):
        return F._xor(other, self)

    def __invert__(self):
        return F._not(self)

    def isIn(self, others):
        return F._in(self, others)

    def isNotIn(self, others):
        return F._notIn(self, others)


class FMeta(type):

    FUNCTION_COMBINATORS = {
        'type_conversion': [
            {'suffix': 'OrZero'},
            {'suffix': 'OrNull'},
        ],
        'aggregate': [
            {'suffix': 'OrDefault'},
            {'suffix': 'OrNull'},
            {'suffix': 'If',          'args': ['cond']},
            {'suffix': 'OrDefaultIf', 'args': ['cond']},
            {'suffix': 'OrNullIf',    'args': ['cond']},
        ],
        'with_utf8_support': [
            {'suffix': 'UTF8'},
        ]
    }

    def __init__(cls, name, bases, dct):
        for name, obj in dct.items():
            if hasattr(obj, '__func__'):
                f_type = getattr(obj.__func__, 'f_type', '')
                for combinator in FMeta.FUNCTION_COMBINATORS.get(f_type, []):
                    new_name = name + combinator['suffix']
                    FMeta._add_func(cls, obj.__func__, new_name, combinator.get('args'))

    @staticmethod
    def _add_func(cls, base_func, new_name, extra_args):
        """
        Adds a new func to the cls, based on the signature of the given base_func but with a new name.
        """
        # Get the function's signature
        sig = signature(base_func)
        new_sig = str(sig)[1 : -1] # omit the parentheses
        args = comma_join(sig.parameters)
        # Add extra args
        if extra_args:
            if args:
                args = comma_join([args] + extra_args)
                new_sig = comma_join([new_sig] + extra_args)
            else:
                args = comma_join(extra_args)
                new_sig = comma_join(extra_args)
        # Get default values for args
        argdefs = tuple(p.default for p in sig.parameters.values() if p.default != Parameter.empty)
        # Build the new function
        new_code = compile('def {new_name}({new_sig}): return F("{new_name}", {args})'.format(**locals()),
                           __file__, 'exec')
        new_func = FunctionType(code=new_code.co_consts[0], globals=globals(), name=new_name, argdefs=argdefs)
        # If base_func was parametric, new_func should be too
        if getattr(base_func, 'f_parametric', False):
            new_func = parametric(new_func)
        # Attach to class
        setattr(cls, new_name, new_func)


class F(Cond, FunctionOperatorsMixin, metaclass=FMeta):
    """
    Represents a database function call and its arguments.
    It doubles as a query condition when the function returns a boolean result.
    """
    def __init__(self, name, *args):
        """
        Initializer.

        """
        self.name = name
        self.args = args
        self.is_binary_operator = False

    def __repr__(self):
        return self.to_sql()

    def to_sql(self, *args):
        """
        Generates an SQL string for this function and its arguments.
        For example if the function name is a symbol of a binary operator:
            (2.54 * `height`)
        For other functions:
            gcd(12, 300)
        """
        if self.is_binary_operator:
            prefix = ''
            sep = ' ' + self.name + ' '
        else:
            prefix = self.name
            sep = ', '
        arg_strs = (arg_to_sql(arg) for arg in self.args if arg != NO_VALUE)
        return prefix + '(' + sep.join(arg_strs) + ')'

    # Arithmetic functions

    @staticmethod
    @binary_operator
    def plus(a, b):
        return F('+', a, b)

    @staticmethod
    @binary_operator
    def minus(a, b):
        return F('-', a, b)

    @staticmethod
    @binary_operator
    def multiply(a, b):
        return F('*', a, b)

    @staticmethod
    @binary_operator
    def divide(a, b):
        return F('/', a, b)

    @staticmethod
    def intDiv(a, b):
        return F('intDiv', a, b)

    @staticmethod
    def intDivOrZero(a, b):
        return F('intDivOrZero', a, b)

    @staticmethod
    @binary_operator
    def modulo(a, b):
        return F('%', a, b)

    @staticmethod
    def negate(a):
        return F('negate', a)

    @staticmethod
    def abs(a):
        return F('abs', a)

    @staticmethod
    def gcd(a, b):
        return F('gcd', a, b)

    @staticmethod
    def lcm(a, b):
        return F('lcm', a, b)

    # Comparison functions

    @staticmethod
    @binary_operator
    def equals(a, b):
        return F('=', a, b)

    @staticmethod
    @binary_operator
    def notEquals(a, b):
        return F('!=', a, b)

    @staticmethod
    @binary_operator
    def less(a, b):
        return F('<', a, b)

    @staticmethod
    @binary_operator
    def greater(a, b):
        return F('>', a, b)

    @staticmethod
    @binary_operator
    def lessOrEquals(a, b):
        return F('<=', a, b)

    @staticmethod
    @binary_operator
    def greaterOrEquals(a, b):
        return F('>=', a, b)

    # Logical functions (should be used as python operators: & | ^ ~)

    @staticmethod
    @binary_operator
    def _and(a, b):
        return F('AND', a, b)

    @staticmethod
    @binary_operator
    def _or(a, b):
        return F('OR', a, b)

    @staticmethod
    def _xor(a, b):
        return F('xor', a, b)

    @staticmethod
    def _not(a):
        return F('not', a)

    # in / not in

    @staticmethod
    @binary_operator
    def _in(a, b):
        if is_iterable(b) and not isinstance(b, (tuple, QuerySet)):
            b = tuple(b)
        return F('IN', a, b)

    @staticmethod
    @binary_operator
    def _notIn(a, b):
        if is_iterable(b) and not isinstance(b, (tuple, QuerySet)):
            b = tuple(b)
        return F('NOT IN', a, b)

    # Functions for working with dates and times

    @staticmethod
    def toYear(d):
        return F('toYear', d)

    @staticmethod
    def toISOYear(d, timezone=NO_VALUE):
        return F('toISOYear', d, timezone)

    @staticmethod
    def toQuarter(d, timezone=NO_VALUE):
        return F('toQuarter', d, timezone) if timezone else F('toQuarter', d)

    @staticmethod
    def toMonth(d):
        return F('toMonth', d)

    @staticmethod
    def toWeek(d, mode=0, timezone=NO_VALUE):
        return F('toWeek', d, mode, timezone)

    @staticmethod
    def toISOWeek(d, timezone=NO_VALUE):
        return F('toISOWeek', d, timezone) if timezone else F('toISOWeek', d)

    @staticmethod
    def toDayOfYear(d):
        return F('toDayOfYear', d)

    @staticmethod
    def toDayOfMonth(d):
        return F('toDayOfMonth', d)

    @staticmethod
    def toDayOfWeek(d):
        return F('toDayOfWeek', d)

    @staticmethod
    def toHour(d):
        return F('toHour', d)

    @staticmethod
    def toMinute(d):
        return F('toMinute', d)

    @staticmethod
    def toSecond(d):
        return F('toSecond', d)

    @staticmethod
    def toMonday(d):
        return F('toMonday', d)

    @staticmethod
    def toStartOfMonth(d):
        return F('toStartOfMonth', d)

    @staticmethod
    def toStartOfQuarter(d):
        return F('toStartOfQuarter', d)

    @staticmethod
    def toStartOfYear(d):
        return F('toStartOfYear', d)

    @staticmethod
    def toStartOfISOYear(d):
        return F('toStartOfISOYear', d)

    @staticmethod
    def toStartOfTenMinutes(d):
        return F('toStartOfTenMinutes', d)

    @staticmethod
    def toStartOfWeek(d, mode=0):
        return F('toStartOfWeek', d)

    @staticmethod
    def toStartOfMinute(d):
        return F('toStartOfMinute', d)

    @staticmethod
    def toStartOfFiveMinute(d):
        return F('toStartOfFiveMinute', d)

    @staticmethod
    def toStartOfFifteenMinutes(d):
        return F('toStartOfFifteenMinutes', d)

    @staticmethod
    def toStartOfHour(d):
        return F('toStartOfHour', d)

    @staticmethod
    def toStartOfDay(d):
        return F('toStartOfDay', d)

    @staticmethod
    def toTime(d, timezone=NO_VALUE):
        return F('toTime', d, timezone)

    @staticmethod
    def toTimeZone(dt, timezone):
        return F('toTimeZone', dt, timezone)

    @staticmethod
    def toUnixTimestamp(dt, timezone=NO_VALUE):
        return F('toUnixTimestamp', dt, timezone)

    @staticmethod
    def toYYYYMM(dt, timezone=NO_VALUE):
        return F('toYYYYMM', dt, timezone) if timezone else F('toYYYYMM', dt)

    @staticmethod
    def toYYYYMMDD(dt, timezone=NO_VALUE):
        return F('toYYYYMMDD', dt, timezone) if timezone else F('toYYYYMMDD', dt)

    @staticmethod
    def toYYYYMMDDhhmmss(dt, timezone=NO_VALUE):
        return F('toYYYYMMDDhhmmss', dt, timezone) if timezone else F('toYYYYMMDDhhmmss', dt)

    @staticmethod
    def toRelativeYearNum(d, timezone=NO_VALUE):
        return F('toRelativeYearNum', d, timezone)

    @staticmethod
    def toRelativeMonthNum(d, timezone=NO_VALUE):
        return F('toRelativeMonthNum', d, timezone)

    @staticmethod
    def toRelativeWeekNum(d, timezone=NO_VALUE):
        return F('toRelativeWeekNum', d, timezone)

    @staticmethod
    def toRelativeDayNum(d, timezone=NO_VALUE):
        return F('toRelativeDayNum', d, timezone)

    @staticmethod
    def toRelativeHourNum(d, timezone=NO_VALUE):
        return F('toRelativeHourNum', d, timezone)

    @staticmethod
    def toRelativeMinuteNum(d, timezone=NO_VALUE):
        return F('toRelativeMinuteNum', d, timezone)

    @staticmethod
    def toRelativeSecondNum(d, timezone=NO_VALUE):
        return F('toRelativeSecondNum', d, timezone)

    @staticmethod
    def now():
        return F('now')

    @staticmethod
    def today():
        return F('today')

    @staticmethod
    def yesterday():
        return F('yesterday')

    @staticmethod
    def timeSlot(d):
        return F('timeSlot', d)

    @staticmethod
    def timeSlots(start_time, duration):
        return F('timeSlots', start_time, F.toUInt32(duration))

    @staticmethod
    def formatDateTime(d, format, timezone=NO_VALUE):
        return F('formatDateTime', d, format, timezone)

    @staticmethod
    def addDays(d, n, timezone=NO_VALUE):
        return F('addDays', d, n, timezone)

    @staticmethod
    def addHours(d, n, timezone=NO_VALUE):
        return F('addHours', d, n, timezone)

    @staticmethod
    def addMinutes(d, n, timezone=NO_VALUE):
        return F('addMinutes', d, n, timezone)

    @staticmethod
    def addMonths(d, n, timezone=NO_VALUE):
        return F('addMonths', d, n, timezone)

    @staticmethod
    def addQuarters(d, n, timezone=NO_VALUE):
        return F('addQuarters', d, n, timezone)

    @staticmethod
    def addSeconds(d, n, timezone=NO_VALUE):
        return F('addSeconds', d, n, timezone)

    @staticmethod
    def addWeeks(d, n, timezone=NO_VALUE):
        return F('addWeeks', d, n, timezone)

    @staticmethod
    def addYears(d, n, timezone=NO_VALUE):
        return F('addYears', d, n, timezone)

    @staticmethod
    def subtractDays(d, n, timezone=NO_VALUE):
        return F('subtractDays', d, n, timezone)

    @staticmethod
    def subtractHours(d, n, timezone=NO_VALUE):
        return F('subtractHours', d, n, timezone)

    @staticmethod
    def subtractMinutes(d, n, timezone=NO_VALUE):
        return F('subtractMinutes', d, n, timezone)

    @staticmethod
    def subtractMonths(d, n, timezone=NO_VALUE):
        return F('subtractMonths', d, n, timezone)

    @staticmethod
    def subtractQuarters(d, n, timezone=NO_VALUE):
        return F('subtractQuarters', d, n, timezone)

    @staticmethod
    def subtractSeconds(d, n, timezone=NO_VALUE):
        return F('subtractSeconds', d, n, timezone)

    @staticmethod
    def subtractWeeks(d, n, timezone=NO_VALUE):
        return F('subtractWeeks', d, n, timezone)

    @staticmethod
    def subtractYears(d, n, timezone=NO_VALUE):
        return F('subtractYears', d, n, timezone)

    @staticmethod
    def toIntervalSecond(number):
        return F('toIntervalSecond', number)

    @staticmethod
    def toIntervalMinute(number):
        return F('toIntervalMinute', number)

    @staticmethod
    def toIntervalHour(number):
        return F('toIntervalHour', number)

    @staticmethod
    def toIntervalDay(number):
        return F('toIntervalDay', number)

    @staticmethod
    def toIntervalWeek(number):
        return F('toIntervalWeek', number)

    @staticmethod
    def toIntervalMonth(number):
        return F('toIntervalMonth', number)

    @staticmethod
    def toIntervalQuarter(number):
        return F('toIntervalQuarter', number)

    @staticmethod
    def toIntervalYear(number):
        return F('toIntervalYear', number)


    # Type conversion functions

    @staticmethod
    @type_conversion
    def toUInt8(x):
        return F('toUInt8', x)

    @staticmethod
    @type_conversion
    def toUInt16(x):
        return F('toUInt16', x)

    @staticmethod
    @type_conversion
    def toUInt32(x):
        return F('toUInt32', x)

    @staticmethod
    @type_conversion
    def toUInt64(x):
        return F('toUInt64', x)

    @staticmethod
    @type_conversion
    def toInt8(x):
        return F('toInt8', x)

    @staticmethod
    @type_conversion
    def toInt16(x):
        return F('toInt16', x)

    @staticmethod
    @type_conversion
    def toInt32(x):
        return F('toInt32', x)

    @staticmethod
    @type_conversion
    def toInt64(x):
        return F('toInt64', x)

    @staticmethod
    @type_conversion
    def toFloat32(x):
        return F('toFloat32', x)

    @staticmethod
    @type_conversion
    def toFloat64(x):
        return F('toFloat64', x)

    @staticmethod
    @type_conversion
    def toDecimal32(x, scale):
        return F('toDecimal32', x, scale)

    @staticmethod
    @type_conversion
    def toDecimal64(x, scale):
        return F('toDecimal64', x, scale)

    @staticmethod
    @type_conversion
    def toDecimal128(x, scale):
        return F('toDecimal128', x, scale)

    @staticmethod
    @type_conversion
    def toDate(x):
        return F('toDate', x)

    @staticmethod
    @type_conversion
    def toDateTime(x):
        return F('toDateTime', x)

    @staticmethod
    @type_conversion
    def toDateTime64(x, precision, timezone=NO_VALUE):
        return F('toDateTime64', x, precision, timezone)

    @staticmethod
    def toString(x):
        return F('toString', x)

    @staticmethod
    def toFixedString(s, length):
        return F('toFixedString', s, length)

    @staticmethod
    def toStringCutToZero(s):
        return F('toStringCutToZero', s)

    @staticmethod
    def CAST(x, type):
        return F('CAST', x, type)

    @staticmethod
    @type_conversion
    def parseDateTimeBestEffort(d, timezone=NO_VALUE):
        return F('parseDateTimeBestEffort', d, timezone)

    # Functions for working with strings

    @staticmethod
    def empty(s):
        return F('empty', s)

    @staticmethod
    def notEmpty(s):
        return F('notEmpty', s)

    @staticmethod
    @with_utf8_support
    def length(s):
        return F('length', s)

    @staticmethod
    @with_utf8_support
    def lower(s):
        return F('lower', s)

    @staticmethod
    @with_utf8_support
    def upper(s):
        return F('upper', s)

    @staticmethod
    @with_utf8_support
    def reverse(s):
        return F('reverse', s)

    @staticmethod
    def concat(*args):
        return F('concat', *args)

    @staticmethod
    @with_utf8_support
    def substring(s, offset, length):
        return F('substring', s, offset, length)

    @staticmethod
    def appendTrailingCharIfAbsent(s, c):
        return F('appendTrailingCharIfAbsent', s, c)

    @staticmethod
    def convertCharset(s, from_charset, to_charset):
        return F('convertCharset', s, from_charset, to_charset)

    @staticmethod
    def base64Encode(s):
        return F('base64Encode', s)

    @staticmethod
    def base64Decode(s):
        return F('base64Decode', s)

    @staticmethod
    def tryBase64Decode(s):
        return F('tryBase64Decode', s)

    @staticmethod
    def endsWith(s, suffix):
        return F('endsWith', s, suffix)

    @staticmethod
    def startsWith(s, prefix):
        return F('startsWith', s, prefix)

    @staticmethod
    def trimLeft(s):
        return F('trimLeft', s)

    @staticmethod
    def trimRight(s):
        return F('trimRight', s)

    @staticmethod
    def trimBoth(s):
        return F('trimBoth', s)

    @staticmethod
    def CRC32(s):
        return F('CRC32', s)

    # Functions for searching in strings

    @staticmethod
    @with_utf8_support
    def position(haystack, needle):
        return F('position', haystack, needle)

    @staticmethod
    @with_utf8_support
    def positionCaseInsensitive(haystack, needle):
        return F('positionCaseInsensitive', haystack, needle)

    @staticmethod
    def like(haystack, pattern):
        return F('like', haystack, pattern)

    @staticmethod
    def notLike(haystack, pattern):
        return F('notLike', haystack, pattern)

    @staticmethod
    def match(haystack, pattern):
        return F('match', haystack, pattern)

    @staticmethod
    def extract(haystack, pattern):
        return F('extract', haystack, pattern)

    @staticmethod
    def extractAll(haystack, pattern):
        return F('extractAll', haystack, pattern)

    @staticmethod
    @with_utf8_support
    def ngramDistance(haystack, needle):
        return F('ngramDistance', haystack, needle)

    @staticmethod
    @with_utf8_support
    def ngramDistanceCaseInsensitive(haystack, needle):
        return F('ngramDistanceCaseInsensitive', haystack, needle)

    @staticmethod
    @with_utf8_support
    def ngramSearch(haystack, needle):
        return F('ngramSearch', haystack, needle)

    @staticmethod
    @with_utf8_support
    def ngramSearchCaseInsensitive(haystack, needle):
        return F('ngramSearchCaseInsensitive', haystack, needle)

    # Functions for replacing in strings

    @staticmethod
    def replace(haystack, pattern, replacement):
        return F('replace', haystack, pattern, replacement)
    replaceAll = replace

    @staticmethod
    def replaceAll(haystack, pattern, replacement):
        return F('replaceAll', haystack, pattern, replacement)

    @staticmethod
    def replaceOne(haystack, pattern, replacement):
        return F('replaceOne', haystack, pattern, replacement)

    @staticmethod
    def replaceRegexpAll(haystack, pattern, replacement):
        return F('replaceRegexpAll', haystack, pattern, replacement)

    @staticmethod
    def replaceRegexpOne(haystack, pattern, replacement):
        return F('replaceRegexpOne', haystack, pattern, replacement)

    @staticmethod
    def regexpQuoteMeta(x):
        return F('regexpQuoteMeta', x)

    # Mathematical functions

    @staticmethod
    def e():
        return F('e')

    @staticmethod
    def pi():
        return F('pi')

    @staticmethod
    def exp(x):
        return F('exp', x)

    @staticmethod
    def log(x):
        return F('log', x)
    ln = log

    @staticmethod
    def exp2(x):
        return F('exp2', x)

    @staticmethod
    def log2(x):
        return F('log2', x)

    @staticmethod
    def exp10(x):
        return F('exp10', x)

    @staticmethod
    def log10(x):
        return F('log10', x)

    @staticmethod
    def sqrt(x):
        return F('sqrt', x)

    @staticmethod
    def cbrt(x):
        return F('cbrt', x)

    @staticmethod
    def erf(x):
        return F('erf', x)

    @staticmethod
    def erfc(x):
        return F('erfc', x)

    @staticmethod
    def lgamma(x):
        return F('lgamma', x)

    @staticmethod
    def tgamma(x):
        return F('tgamma', x)

    @staticmethod
    def sin(x):
        return F('sin', x)

    @staticmethod
    def cos(x):
        return F('cos', x)

    @staticmethod
    def tan(x):
        return F('tan', x)

    @staticmethod
    def asin(x):
        return F('asin', x)

    @staticmethod
    def acos(x):
        return F('acos', x)

    @staticmethod
    def atan(x):
        return F('atan', x)

    @staticmethod
    def power(x, y):
        return F('power', x, y)
    pow = power

    @staticmethod
    def intExp10(x):
        return F('intExp10', x)

    @staticmethod
    def intExp2(x):
        return F('intExp2', x)

    # Rounding functions

    @staticmethod
    def floor(x, n=None):
        return F('floor', x, n) if n else F('floor', x)

    @staticmethod
    def ceiling(x, n=None):
        return F('ceiling', x, n) if n else F('ceiling', x)
    ceil = ceiling

    @staticmethod
    def round(x, n=None):
        return F('round', x, n) if n else F('round', x)

    @staticmethod
    def roundAge(x):
        return F('roundAge', x)

    @staticmethod
    def roundDown(x, y):
        return F('roundDown', x, y)

    @staticmethod
    def roundDuration(x):
        return F('roundDuration', x)

    @staticmethod
    def roundToExp2(x):
        return F('roundToExp2', x)

    # Functions for working with arrays

    @staticmethod
    def emptyArrayDate():
        return F('emptyArrayDate')

    @staticmethod
    def emptyArrayDateTime():
        return F('emptyArrayDateTime')

    @staticmethod
    def emptyArrayFloat32():
        return F('emptyArrayFloat32')

    @staticmethod
    def emptyArrayFloat64():
        return F('emptyArrayFloat64')

    @staticmethod
    def emptyArrayInt16():
        return F('emptyArrayInt16')

    @staticmethod
    def emptyArrayInt32():
        return F('emptyArrayInt32')

    @staticmethod
    def emptyArrayInt64():
        return F('emptyArrayInt64')

    @staticmethod
    def emptyArrayInt8():
        return F('emptyArrayInt8')

    @staticmethod
    def emptyArrayString():
        return F('emptyArrayString')

    @staticmethod
    def emptyArrayUInt16():
        return F('emptyArrayUInt16')

    @staticmethod
    def emptyArrayUInt32():
        return F('emptyArrayUInt32')

    @staticmethod
    def emptyArrayUInt64():
        return F('emptyArrayUInt64')

    @staticmethod
    def emptyArrayUInt8():
        return F('emptyArrayUInt8')

    @staticmethod
    def emptyArrayToSingle(x):
        return F('emptyArrayToSingle', x)

    @staticmethod
    def range(n):
        return F('range', n)

    @staticmethod
    def array(*args):
        return F('array', *args)

    @staticmethod
    def arrayConcat(*args):
        return F('arrayConcat', *args)

    @staticmethod
    def arrayElement(arr, n):
        return F('arrayElement', arr, n)

    @staticmethod
    def has(arr, x):
        return F('has', arr, x)

    @staticmethod
    def hasAll(arr, x):
        return F('hasAll', arr, x)

    @staticmethod
    def hasAny(arr, x):
        return F('hasAny', arr, x)

    @staticmethod
    def indexOf(arr, x):
        return F('indexOf', arr, x)

    @staticmethod
    def countEqual(arr, x):
        return F('countEqual', arr, x)

    @staticmethod
    def arrayEnumerate(arr):
        return F('arrayEnumerate', arr)

    @staticmethod
    def arrayEnumerateDense(*args):
        return F('arrayEnumerateDense', *args)

    @staticmethod
    def arrayEnumerateDenseRanked(*args):
        return F('arrayEnumerateDenseRanked', *args)

    @staticmethod
    def arrayEnumerateUniq(*args):
        return F('arrayEnumerateUniq', *args)

    @staticmethod
    def arrayEnumerateUniqRanked(*args):
        return F('arrayEnumerateUniqRanked', *args)

    @staticmethod
    def arrayPopBack(arr):
        return F('arrayPopBack', arr)

    @staticmethod
    def arrayPopFront(arr):
        return F('arrayPopFront', arr)

    @staticmethod
    def arrayPushBack(arr, x):
        return F('arrayPushBack', arr, x)

    @staticmethod
    def arrayPushFront(arr, x):
        return F('arrayPushFront', arr, x)

    @staticmethod
    def arrayResize(array, size, extender=None):
        return F('arrayResize', array, size, extender) if extender is not None else F('arrayResize', array, size)

    @staticmethod
    def arraySlice(array, offset, length=None):
        return F('arraySlice', array, offset, length) if length is not None else F('arraySlice', array, offset)

    @staticmethod
    def arrayUniq(*args):
        return F('arrayUniq', *args)

    @staticmethod
    def arrayJoin(arr):
        return F('arrayJoin', arr)

    @staticmethod
    def arrayDifference(arr):
        return F('arrayDifference', arr)

    @staticmethod
    def arrayDistinct(x):
        return F('arrayDistinct', x)

    @staticmethod
    def arrayIntersect(*args):
        return F('arrayIntersect', *args)

    @staticmethod
    def arrayReduce(agg_func_name, *args):
        return F('arrayReduce', agg_func_name, *args)

    @staticmethod
    def arrayReverse(arr):
        return F('arrayReverse', arr)

    # Functions for splitting and merging strings and arrays

    @staticmethod
    def splitByChar(sep, s):
        return F('splitByChar', sep, s)

    @staticmethod
    def splitByString(sep, s):
        return F('splitByString', sep, s)

    @staticmethod
    def arrayStringConcat(arr, sep=None):
        return F('arrayStringConcat', arr, sep) if sep else F('arrayStringConcat', arr)

    @staticmethod
    def alphaTokens(s):
        return F('alphaTokens', s)

    # Bit functions

    @staticmethod
    def bitAnd(x, y):
        return F('bitAnd', x, y)

    @staticmethod
    def bitNot(x):
        return F('bitNot', x)

    @staticmethod
    def bitOr(x, y):
        return F('bitOr', x, y)

    @staticmethod
    def bitRotateLeft(x, y):
        return F('bitRotateLeft', x, y)

    @staticmethod
    def bitRotateRight(x, y):
        return F('bitRotateRight', x, y)

    @staticmethod
    def bitShiftLeft(x, y):
        return F('bitShiftLeft', x, y)

    @staticmethod
    def bitShiftRight(x, y):
        return F('bitShiftRight', x, y)

    @staticmethod
    def bitTest(x, y):
        return F('bitTest', x, y)

    @staticmethod
    def bitTestAll(x, *args):
        return F('bitTestAll', x, *args)

    @staticmethod
    def bitTestAny(x, *args):
        return F('bitTestAny', x, *args)

    @staticmethod
    def bitXor(x, y):
        return F('bitXor', x, y)

    # Bitmap functions

    @staticmethod
    def bitmapAnd(x, y):
        return F('bitmapAnd', x, y)

    @staticmethod
    def bitmapAndCardinality(x, y):
        return F('bitmapAndCardinality', x, y)

    @staticmethod
    def bitmapAndnot(x, y):
        return F('bitmapAndnot', x, y)

    @staticmethod
    def bitmapAndnotCardinality(x, y):
        return F('bitmapAndnotCardinality', x, y)

    @staticmethod
    def bitmapBuild(x):
        return F('bitmapBuild', x)

    @staticmethod
    def bitmapCardinality(x):
        return F('bitmapCardinality', x)

    @staticmethod
    def bitmapContains(haystack, needle):
        return F('bitmapContains', haystack, needle)

    @staticmethod
    def bitmapHasAll(x, y):
        return F('bitmapHasAll', x, y)

    @staticmethod
    def bitmapHasAny(x, y):
        return F('bitmapHasAny', x, y)

    @staticmethod
    def bitmapOr(x, y):
        return F('bitmapOr', x, y)

    @staticmethod
    def bitmapOrCardinality(x, y):
        return F('bitmapOrCardinality', x, y)

    @staticmethod
    def bitmapToArray(x):
        return F('bitmapToArray', x)

    @staticmethod
    def bitmapXor(x, y):
        return F('bitmapXor', x, y)

    @staticmethod
    def bitmapXorCardinality(x, y):
        return F('bitmapXorCardinality', x, y)

    # Hash functions

    @staticmethod
    def halfMD5(*args):
        return F('halfMD5', *args)

    @staticmethod
    def MD5(s):
        return F('MD5', s)

    @staticmethod
    def sipHash128(*args):
        return F('sipHash128', *args)

    @staticmethod
    def sipHash64(*args):
        return F('sipHash64', *args)

    @staticmethod
    def cityHash64(*args):
        return F('cityHash64', *args)

    @staticmethod
    def intHash32(x):
        return F('intHash32', x)

    @staticmethod
    def intHash64(x):
        return F('intHash64', x)

    @staticmethod
    def SHA1(s):
        return F('SHA1', s)

    @staticmethod
    def SHA224(s):
        return F('SHA224', s)

    @staticmethod
    def SHA256(s):
        return F('SHA256', s)

    @staticmethod
    def URLHash(url, n=None):
        return F('URLHash', url, n) if n is not None else F('URLHash', url)

    @staticmethod
    def farmHash64(*args):
        return F('farmHash64',*args)

    @staticmethod
    def javaHash(s):
        return F('javaHash', s)

    @staticmethod
    def hiveHash(s):
        return F('hiveHash', s)

    @staticmethod
    def metroHash64(*args):
        return F('metroHash64', *args)

    @staticmethod
    def jumpConsistentHash(x, buckets):
        return F('jumpConsistentHash', x, buckets)

    @staticmethod
    def murmurHash2_32(*args):
        return F('murmurHash2_32', *args)

    @staticmethod
    def murmurHash2_64(*args):
        return F('murmurHash2_64', *args)

    @staticmethod
    def murmurHash3_32(*args):
        return F('murmurHash3_32', *args)

    @staticmethod
    def murmurHash3_64(*args):
        return F('murmurHash3_64', *args)

    @staticmethod
    def murmurHash3_128(s):
        return F('murmurHash3_128', s)

    @staticmethod
    def xxHash32(*args):
        return F('xxHash32', *args)

    @staticmethod
    def xxHash64(*args):
        return F('xxHash64', *args)

    # Functions for generating pseudo-random numbers

    @staticmethod
    def rand(dummy=None):
        return F('rand') if dummy is None else F('rand', dummy)

    @staticmethod
    def rand64(dummy=None):
        return F('rand64') if dummy is None else F('rand64', dummy)

    @staticmethod
    def randConstant(dummy=None):
        return F('randConstant') if dummy is None else F('randConstant', dummy)

    # Encoding functions

    @staticmethod
    def hex(x):
        return F('hex', x)

    @staticmethod
    def unhex(x):
        return F('unhex', x)

    @staticmethod
    def bitmaskToArray(x):
        return F('bitmaskToArray', x)

    @staticmethod
    def bitmaskToList(x):
        return F('bitmaskToList', x)

    # Functions for working with UUID

    @staticmethod
    def generateUUIDv4():
        return F('generateUUIDv4')

    @staticmethod
    def toUUID(s):
        return F('toUUID', s)

    @staticmethod
    def UUIDNumToString(s):
        return F('UUIDNumToString', s)

    @staticmethod
    def UUIDStringToNum(s):
        return F('UUIDStringToNum', s)

    # Functions for working with IP addresses

    @staticmethod
    def IPv4CIDRToRange(ipv4, cidr):
        return F('IPv4CIDRToRange', ipv4, cidr)

    @staticmethod
    def IPv4NumToString(num):
        return F('IPv4NumToString', num)

    @staticmethod
    def IPv4NumToStringClassC(num):
        return F('IPv4NumToStringClassC', num)

    @staticmethod
    def IPv4StringToNum(s):
        return F('IPv4StringToNum', s)

    @staticmethod
    def IPv4ToIPv6(ipv4):
        return F('IPv4ToIPv6', ipv4)

    @staticmethod
    def IPv6CIDRToRange(ipv6, cidr):
        return F('IPv6CIDRToRange', ipv6, cidr)

    @staticmethod
    def IPv6NumToString(num):
        return F('IPv6NumToString', num)

    @staticmethod
    def IPv6StringToNum(s):
        return F('IPv6StringToNum', s)

    @staticmethod
    def toIPv4(ipv4):
        return F('toIPv4', ipv4)

    @staticmethod
    def toIPv6(ipv6):
        return F('toIPv6', ipv6)

    # Aggregate functions

    @staticmethod
    @aggregate
    def any(x):
        return F('any', x)

    @staticmethod
    @aggregate
    def anyHeavy(x):
        return F('anyHeavy', x)

    @staticmethod
    @aggregate
    def anyLast(x):
        return F('anyLast', x)

    @staticmethod
    @aggregate
    def argMax(x, y):
        return F('argMax', x, y)

    @staticmethod
    @aggregate
    def argMin(x, y):
        return F('argMin', x, y)

    @staticmethod
    @aggregate
    def avg(x):
        return F('avg', x)

    @staticmethod
    @aggregate
    def corr(x, y):
        return F('corr', x, y)

    @staticmethod
    @aggregate
    def count():
        return F('count')

    @staticmethod
    @aggregate
    def covarPop(x, y):
        return F('covarPop', x, y)

    @staticmethod
    @aggregate
    def covarSamp(x, y):
        return F('covarSamp', x, y)

    @staticmethod
    @aggregate
    def kurtPop(x):
        return F('kurtPop', x)

    @staticmethod
    @aggregate
    def kurtSamp(x):
        return F('kurtSamp', x)

    @staticmethod
    @aggregate
    def min(x):
        return F('min', x)

    @staticmethod
    @aggregate
    def max(x):
        return F('max', x)

    @staticmethod
    @aggregate
    def skewPop(x):
        return F('skewPop', x)

    @staticmethod
    @aggregate
    def skewSamp(x):
        return F('skewSamp', x)

    @staticmethod
    @aggregate
    def sum(x):
        return F('sum', x)

    @staticmethod
    @aggregate
    def uniq(*args):
        return F('uniq', *args)

    @staticmethod
    @aggregate
    def uniqExact(*args):
        return F('uniqExact', *args)

    @staticmethod
    @aggregate
    def uniqHLL12(*args):
        return F('uniqHLL12', *args)

    @staticmethod
    @aggregate
    def varPop(x):
        return F('varPop', x)

    @staticmethod
    @aggregate
    def varSamp(x):
        return F('varSamp', x)

    @staticmethod
    @aggregate
    def stddevPop(expr):
        return F('stddevPop', expr)

    @staticmethod
    @aggregate
    def stddevSamp(expr):
        return F('stddevSamp', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantile(expr):
        return F('quantile', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantileDeterministic(expr, determinator):
        return F('quantileDeterministic', expr, determinator)

    @staticmethod
    @aggregate
    @parametric
    def quantileExact(expr):
        return F('quantileExact', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantileExactWeighted(expr, weight):
        return F('quantileExactWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def quantileTiming(expr):
        return F('quantileTiming', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantileTimingWeighted(expr, weight):
        return F('quantileTimingWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def quantileTDigest(expr):
        return F('quantileTDigest', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantileTDigestWeighted(expr, weight):
        return F('quantileTDigestWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def quantiles(expr):
        return F('quantiles', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantilesDeterministic(expr, determinator):
        return F('quantilesDeterministic', expr, determinator)

    @staticmethod
    @aggregate
    @parametric
    def quantilesExact(expr):
        return F('quantilesExact', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantilesExactWeighted(expr, weight):
        return F('quantilesExactWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def quantilesTiming(expr):
        return F('quantilesTiming', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantilesTimingWeighted(expr, weight):
        return F('quantilesTimingWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def quantilesTDigest(expr):
        return F('quantilesTDigest', expr)

    @staticmethod
    @aggregate
    @parametric
    def quantilesTDigestWeighted(expr, weight):
        return F('quantilesTDigestWeighted', expr, weight)

    @staticmethod
    @aggregate
    @parametric
    def topK(expr):
        return F('topK', expr)

    @staticmethod
    @aggregate
    @parametric
    def topKWeighted(expr, weight):
        return F('topKWeighted', expr, weight)

    # Null handling functions

    @staticmethod
    def ifNull(x, y):
        return F('ifNull', x, y)

    @staticmethod
    def nullIf(x, y):
        return F('nullIf', x, y)

    @staticmethod
    def isNotNull(x):
        return F('isNotNull', x)

    @staticmethod
    def isNull(x):
        return F('isNull', x)

    @staticmethod
    def coalesce(*args):
        return F('coalesce', *args)

    # Misc functions

    @staticmethod
    def ifNotFinite(x, y):
        return F('ifNotFinite', x, y)

    @staticmethod
    def isFinite(x):
        return F('isFinite', x)

    @staticmethod
    def isInfinite(x):
        return F('isInfinite', x)

    @staticmethod
    def isNaN(x):
        return F('isNaN', x)

    @staticmethod
    def least(x, y):
        return F('least', x, y)

    @staticmethod
    def greatest(x, y):
        return F('greatest', x, y)

    # Dictionary functions

    @staticmethod
    def dictGet(dict_name, attr_name, id_expr):
        return F('dictGet', dict_name, attr_name, id_expr)

    @staticmethod
    def dictGetOrDefault(dict_name, attr_name, id_expr, default):
        return F('dictGetOrDefault', dict_name, attr_name, id_expr, default)

    @staticmethod
    def dictHas(dict_name, id_expr):
        return F('dictHas', dict_name, id_expr)

    @staticmethod
    def dictGetHierarchy(dict_name, id_expr):
        return F('dictGetHierarchy', dict_name, id_expr)

    @staticmethod
    def dictIsIn(dict_name, child_id_expr, ancestor_id_expr):
        return F('dictIsIn', dict_name, child_id_expr, ancestor_id_expr)


# Expose only relevant classes in import *
__all__ = ['F']


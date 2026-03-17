"""Module containing canned measurement validators.

Additional validators may be registered by passing them to the Register()
method.  They can then be accessed directly as attributes on the validators
module, and will typically be a type, instances of which are callable:

  from openhtf.util import validators
  from openhtf.util import measurements

  class MyLessThanValidator(ValidatorBase):
    def __init__(self, limit):
      self.limit = limit

    # This will be invoked to test if the measurement is 'PASS' or 'FAIL'.
    def __call__(self, value):
      return value < self.limit

  # Name defaults to the validator's __name__ attribute unless overridden.
  validators.register(MyLessThanValidator, name='LessThan')

  # Now you can refer to the validator by name directly on measurements.
  @measurements.measures(
      measurements.Measurement('my_measurement').LessThan(4))
  def MyPhase(test):
    test.measurements.my_measurement = 5  # Will have outcome 'FAIL'

If implemented as a class, inherit from a suitable base class defined in this
module; such validators may have specialized handling by the infrastructure that
you can leverage.

For simpler validators, you don't need to register them at all, you can
simply attach them to the Measurement with the .with_validator() method:

  def LessThan4(value):
    return value < 4

  @measurements.measures(
      measurements.Measurement('my_measurement).with_validator(LessThan4))
  def MyPhase(test):
    test.measurements.my_measurement = 5  # Will also 'FAIL'

Notes:

Note the extra level of indirection when registering a validator. This allows
you to parameterize your validator (like in the LessThan example) when it is
being applied to the measurement.  If you don't need this level of indirection,
it's recommended that you simply use .with_validator() instead.

Also note the validator will be str()'d in the output, so if you want a
meaningful description of what it does, you should implement a __str__ method.

Validators must also be deepcopy()'able, and may need to implement __deepcopy__
if they are implemented by a class that has internal state that is not copyable
by the default copy.deepcopy().
"""

import abc
import numbers
import re
from future.utils import with_metaclass
from openhtf import util
import six

_VALIDATORS = {}


def register(validator, name=None):
  name = name or validator.__name__
  if name in _VALIDATORS:
    raise ValueError('Duplicate validator name', name)
  _VALIDATORS[name] = validator
  return validator


def has_validator(name):
  return name in _VALIDATORS


def create_validator(name, *args, **kwargs):
  return _VALIDATORS[name](*args, **kwargs)

_identity = lambda x: x


class ValidatorBase(with_metaclass(abc.ABCMeta, object)):
  @abc.abstractmethod
  def __call__(self, value):
    """Should validate value, returning a boolean result."""


class RangeValidatorBase(with_metaclass(abc.ABCMeta, ValidatorBase)):
  @abc.abstractproperty
  def minimum(self):
    """Should return the minimum, inclusive value of the range."""

  @abc.abstractproperty
  def maximum(self):
    """Should return the maximum, inclusive value of the range."""


# Built-in validators below this line

class InRange(RangeValidatorBase):
  """Validator to verify a numeric value is within a range."""

  def __init__(self, minimum=None, maximum=None, type=None):
    if minimum is None and maximum is None:
      raise ValueError('Must specify minimum, maximum, or both')
    if (minimum is not None and maximum is not None
        and isinstance(minimum, numbers.Number)
        and isinstance(maximum, numbers.Number)
        and minimum > maximum):
      raise ValueError('Minimum cannot be greater than maximum')
    self._minimum = minimum
    self._maximum = maximum
    self._type = type

  @property
  def minimum(self):
    converter = self._type if self._type is not None else _identity
    return converter(self._minimum)

  @property
  def maximum(self):
    converter = self._type if self._type is not None else _identity
    return converter(self._maximum)

  def with_args(self, **kwargs):
    return type(self)(
        minimum=util.format_string(self._minimum, kwargs),
        maximum=util.format_string(self._maximum, kwargs),
        type=self._type,
    )

  def __call__(self, value):
    if value is None:
      return False
    import math
    # Check for nan
    if math.isnan(value):
      return False
    if self.minimum is not None and value < self.minimum:
      return False
    if self.maximum is not None and value > self.maximum:
      return False
    return True

  def __str__(self):
    assert self._minimum is not None or self._maximum is not None
    if self._minimum is not None and self._maximum is not None:
      if self._minimum == self._maximum:
        return 'x == %s' % self._minimum
      return '%s <= x <= %s' % (self._minimum, self._maximum)
    if self._minimum is not None:
      return '%s <= x' % self._minimum
    if self._maximum is not None:
      return 'x <= %s' % self._maximum

  def __eq__(self, other):
    return (isinstance(other, type(self)) and
            self.minimum == other.minimum and self.maximum == other.maximum)

  def __ne__(self, other):
    return not self == other

in_range = InRange  # pylint: disable=invalid-name
register(in_range, name='in_range')


@register
def equals(value, type=None):
  if isinstance(value, numbers.Number):
    return InRange(minimum=value, maximum=value, type=type)
  elif isinstance(value, six.string_types):
    assert type is None or issubclass(type, six.string_types), (
        'Cannot use a non-string type when matching a string')
    return matches_regex('^{}$'.format(re.escape(value)))
  else:
    return Equals(value, type=type)


class Equals(object):
  """Validator to verify an object is equal to the expected value."""

  def __init__(self, expected, type=None):
    self._expected = expected
    self._type = type

  @property
  def expected(self):
    converter = self._type if self._type is not None else _identity
    return converter(self._expected)

  def __call__(self, value):
    return value == self.expected

  def __str__(self):
    return "'x' is equal to '%s'" % self._expected

  def __eq__(self, other):
    return isinstance(other, type(self)) and self.expected == other.expected


class RegexMatcher(object):
  """Validator to verify a string value matches a regex."""

  def __init__(self, regex, compiled_regex):
    self._compiled = compiled_regex
    self.regex = regex

  def __call__(self, value):
    return self._compiled.match(str(value)) is not None

  def __deepcopy__(self, dummy_memo):
    return type(self)(self.regex, self._compiled)

  def __str__(self):
    return "'x' matches /%s/" % self.regex

  def __eq__(self, other):
    return isinstance(other, type(self)) and self.regex == other.regex

  def __ne__(self, other):
    return not self == other


@register
def matches_regex(regex):
  return RegexMatcher(regex, re.compile(regex))


class WithinPercent(RangeValidatorBase):
  """Validates that a number is within percent of a value."""

  def __init__(self, expected, percent):
    if percent < 0:
      raise ValueError('percent argument is {}, must be >0'.format(percent))
    self.expected = expected
    self.percent = percent

  @property
  def _applied_percent(self):
    return abs(self.expected * self.percent / 100.0)

  @property
  def minimum(self):
    return self.expected - self._applied_percent

  @property
  def maximum(self):
    return self.expected + self._applied_percent

  def __call__(self, value):
    return self.minimum <= value <= self.maximum

  def __str__(self):
    return "'x' is within {}% of {}".format(self.percent, self.expected)

  def __eq__(self, other):
    return (isinstance(other, type(self)) and
            self.expected == other.expected and
            self.percent == other.percent)

  def __ne__(self, other):
    return not self == other


@register
def within_percent(expected, percent):
  return WithinPercent(expected, percent)

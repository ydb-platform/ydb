# -*- coding: utf-8 -*-
from abc import abstractmethod

from plumbum import local
from plumbum.cli.i18n import get_translation_for
from plumbum.lib import getdoc, six

_translation = get_translation_for(__name__)
_, ngettext = _translation.gettext, _translation.ngettext


class SwitchError(Exception):
    """A general switch related-error (base class of all other switch errors)"""

    pass


class PositionalArgumentsError(SwitchError):
    """Raised when an invalid number of positional arguments has been given"""

    pass


class SwitchCombinationError(SwitchError):
    """Raised when an invalid combination of switches has been given"""

    pass


class UnknownSwitch(SwitchError):
    """Raised when an unrecognized switch has been given"""

    pass


class MissingArgument(SwitchError):
    """Raised when a switch requires an argument, but one was not provided"""

    pass


class MissingMandatorySwitch(SwitchError):
    """Raised when a mandatory switch has not been given"""

    pass


class WrongArgumentType(SwitchError):
    """Raised when a switch expected an argument of some type, but an argument of a wrong
    type has been given"""

    pass


class SubcommandError(SwitchError):
    """Raised when there's something wrong with sub-commands"""

    pass


# ===================================================================================================
# The switch decorator
# ===================================================================================================
class SwitchInfo(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def switch(
    names,
    argtype=None,
    argname=None,
    list=False,
    mandatory=False,
    requires=(),
    excludes=(),
    help=None,
    overridable=False,
    group="Switches",
    envname=None,
):
    """
    A decorator that exposes functions as command-line switches. Usage::

        class MyApp(Application):
            @switch(["-l", "--log-to-file"], argtype = str)
            def log_to_file(self, filename):
                handler = logging.FileHandler(filename)
                logger.addHandler(handler)

            @switch(["--verbose"], excludes=["--terse"], requires=["--log-to-file"])
            def set_debug(self):
                logger.setLevel(logging.DEBUG)

            @switch(["--terse"], excludes=["--verbose"], requires=["--log-to-file"])
            def set_terse(self):
                logger.setLevel(logging.WARNING)

    :param names: The name(s) under which the function is reachable; it can be a string
                  or a list of string, but at least one name is required. There's no need
                  to prefix the name with ``-`` or ``--`` (this is added automatically),
                  but it can be used for clarity. Single-letter names are prefixed by ``-``,
                  while longer names are prefixed by ``--``

    :param envname:   Name of environment variable to extract value from, as alternative to argv

    :param argtype: If this function takes an argument, you need to specify its type. The
                    default is ``None``, which means the function takes no argument. The type
                    is more of a "validator" than a real type; it can be any callable object
                    that raises a ``TypeError`` if the argument is invalid, or returns an
                    appropriate value on success. If the user provides an invalid value,
                    :func:`plumbum.cli.WrongArgumentType`

    :param argname: The name of the argument; if ``None``, the name will be inferred from the
                    function's signature

    :param list: Whether or not this switch can be repeated (e.g. ``gcc -I/lib -I/usr/lib``).
                 If ``False``, only a single occurrence of the switch is allowed; if ``True``,
                 it may be repeated indefinitely. The occurrences are collected into a list,
                 so the function is only called once with the collections. For instance,
                 for ``gcc -I/lib -I/usr/lib``, the function will be called with
                 ``["/lib", "/usr/lib"]``.

    :param mandatory: Whether or not this switch is mandatory; if a mandatory switch is not
                      given, :class:`MissingMandatorySwitch <plumbum.cli.MissingMandatorySwitch>`
                      is raised. The default is ``False``.

    :param requires: A list of switches that this switch depends on ("requires"). This means that
                     it's invalid to invoke this switch without also invoking the required ones.
                     In the example above, it's illegal to pass ``--verbose`` or ``--terse``
                     without also passing ``--log-to-file``. By default, this list is empty,
                     which means the switch has no prerequisites. If an invalid combination
                     is given, :class:`SwitchCombinationError <plumbum.cli.SwitchCombinationError>`
                     is raised.

                     Note that this list is made of the switch *names*; if a switch has more
                     than a single name, any of its names will do.

                     .. note::
                        There is no guarantee on the (topological) order in which the actual
                        switch functions will be invoked, as the dependency graph might contain
                        cycles.

    :param excludes: A list of switches that this switch forbids ("excludes"). This means that
                     it's invalid to invoke this switch if any of the excluded ones are given.
                     In the example above, it's illegal to pass ``--verbose`` along with
                     ``--terse``, as it will result in a contradiction. By default, this list
                     is empty, which means the switch has no prerequisites. If an invalid
                     combination is given, :class:`SwitchCombinationError
                     <plumbum.cli.SwitchCombinationError>` is raised.

                     Note that this list is made of the switch *names*; if a switch has more
                     than a single name, any of its names will do.

    :param help: The help message (description) for this switch; this description is used when
                 ``--help`` is given. If ``None``, the function's docstring will be used.

    :param overridable: Whether or not the names of this switch are overridable by other switches.
                        If ``False`` (the default), having another switch function with the same
                        name(s) will cause an exception. If ``True``, this is silently ignored.

    :param group: The switch's *group*; this is a string that is used to group related switches
                  together when ``--help`` is given. The default group is ``Switches``.

    :returns: The decorated function (with a ``_switch_info`` attribute)
    """
    if isinstance(names, six.string_types):
        names = [names]
    names = [n.lstrip("-") for n in names]
    requires = [n.lstrip("-") for n in requires]
    excludes = [n.lstrip("-") for n in excludes]

    def deco(func):
        if argname is None:
            argspec = six.getfullargspec(func).args
            if len(argspec) == 2:
                argname2 = argspec[1]
            else:
                argname2 = _("VALUE")
        else:
            argname2 = argname
        help2 = getdoc(func) if help is None else help
        if not help2:
            help2 = str(func)
        func._switch_info = SwitchInfo(
            names=names,
            envname=envname,
            argtype=argtype,
            list=list,
            func=func,
            mandatory=mandatory,
            overridable=overridable,
            group=group,
            requires=requires,
            excludes=excludes,
            argname=argname2,
            help=help2,
        )
        return func

    return deco


def autoswitch(*args, **kwargs):
    """A decorator that exposes a function as a switch, "inferring" the name of the switch
    from the function's name (converting to lower-case, and replacing underscores with hyphens).
    The arguments are the same as for :func:`switch <plumbum.cli.switch>`."""

    def deco(func):
        return switch(func.__name__.replace("_", "-"), *args, **kwargs)(func)

    return deco


# ===================================================================================================
# Switch Attributes
# ===================================================================================================
class SwitchAttr(object):
    """
    A switch that stores its result in an attribute (descriptor). Usage::

        class MyApp(Application):
            logfile = SwitchAttr(["-f", "--log-file"], str)

            def main(self):
                if self.logfile:
                    open(self.logfile, "w")

    :param names: The switch names
    :param argtype: The switch argument's (and attribute's) type
    :param default: The attribute's default value (``None``)
    :param argname: The switch argument's name (default is ``"VALUE"``)
    :param kwargs: Any of the keyword arguments accepted by :func:`switch <plumbum.cli.switch>`
    """

    ATTR_NAME = "__plumbum_switchattr_dict__"

    def __init__(
        self, names, argtype=str, default=None, list=False, argname=_("VALUE"), **kwargs
    ):
        self.__doc__ = "Sets an attribute"  # to prevent the help message from showing SwitchAttr's docstring
        if default and argtype is not None:
            defaultmsg = _("; the default is {0}").format(default)
            if "help" in kwargs:
                kwargs["help"] += defaultmsg
            else:
                kwargs["help"] = defaultmsg.lstrip("; ")

        switch(names, argtype=argtype, argname=argname, list=list, **kwargs)(self)
        listtype = type([])
        if list:
            if default is None:
                self._default_value = []
            elif isinstance(default, (tuple, listtype)):
                self._default_value = listtype(default)
            else:
                self._default_value = [default]
        else:
            self._default_value = default

    def __call__(self, inst, val):
        self.__set__(inst, val)

    def __get__(self, inst, cls):
        if inst is None:
            return self
        else:
            return getattr(inst, self.ATTR_NAME, {}).get(self, self._default_value)

    def __set__(self, inst, val):
        if inst is None:
            raise AttributeError("cannot set an unbound SwitchAttr")
        else:
            if not hasattr(inst, self.ATTR_NAME):
                setattr(inst, self.ATTR_NAME, {self: val})
            else:
                getattr(inst, self.ATTR_NAME)[self] = val


class Flag(SwitchAttr):
    """A specialized :class:`SwitchAttr <plumbum.cli.SwitchAttr>` for boolean flags. If the flag is not
    given, the value of this attribute is ``default``; if it is given, the value changes
    to ``not default``. Usage::

        class MyApp(Application):
            verbose = Flag(["-v", "--verbose"], help = "If given, I'll be very talkative")

    :param names: The switch names
    :param default: The attribute's initial value (``False`` by default)
    :param kwargs: Any of the keyword arguments accepted by :func:`switch <plumbum.cli.switch>`,
                   except for ``list`` and ``argtype``.
    """

    def __init__(self, names, default=False, **kwargs):
        SwitchAttr.__init__(
            self, names, argtype=None, default=default, list=False, **kwargs
        )

    def __call__(self, inst):
        self.__set__(inst, not self._default_value)


class CountOf(SwitchAttr):
    """A specialized :class:`SwitchAttr <plumbum.cli.SwitchAttr>` that counts the number of
    occurrences of the switch in the command line. Usage::

        class MyApp(Application):
            verbosity = CountOf(["-v", "--verbose"], help = "The more, the merrier")

    If ``-v -v -vv`` is given in the command-line, it will result in ``verbosity = 4``.

    :param names: The switch names
    :param default: The default value (0)
    :param kwargs: Any of the keyword arguments accepted by :func:`switch <plumbum.cli.switch>`,
                   except for ``list`` and ``argtype``.
    """

    def __init__(self, names, default=0, **kwargs):
        SwitchAttr.__init__(
            self, names, argtype=None, default=default, list=True, **kwargs
        )
        self._default_value = default  # issue #118

    def __call__(self, inst, v):
        self.__set__(inst, len(v))


# ===================================================================================================
# Decorator for function that adds argument checking
# ===================================================================================================


class positional(object):
    """
    Runs a validator on the main function for a class.
    This should be used like this::

        class MyApp(cli.Application):
            @cli.positional(cli.Range(1,10), cli.ExistingFile)
            def main(self, x, *f):
                # x is a range, f's are all ExistingFile's)

    Or, Python 3 only::

        class MyApp(cli.Application):
            def main(self, x : cli.Range(1,10), *f : cli.ExistingFile):
                # x is a range, f's are all ExistingFile's)


    If you do not want to validate on the annotations, use this decorator (
    even if empty) to override annotation validation.

    Validators should be callable, and should have a ``.choices()`` function with
    possible choices. (For future argument completion, for example)

    Default arguments do not go through the validator.

    #TODO: Check with MyPy

    """

    def __init__(self, *args, **kargs):
        self.args = args
        self.kargs = kargs

    def __call__(self, function):
        m = six.getfullargspec(function)
        args_names = list(m.args[1:])

        positional = [None] * len(args_names)
        varargs = None

        for i in range(min(len(positional), len(self.args))):
            positional[i] = self.args[i]

        if len(args_names) + 1 == len(self.args):
            varargs = self.args[-1]

        # All args are positional, so convert kargs to positional
        for item in self.kargs:
            if item == m.varargs:
                varargs = self.kargs[item]
            else:
                positional[args_names.index(item)] = self.kargs[item]

        function.positional = positional
        function.positional_varargs = varargs
        return function


class Validator(six.ABC):
    __slots__ = ()

    @abstractmethod
    def __call__(self, obj):
        "Must be implemented for a Validator to work"

    def choices(self, partial=""):
        """Should return set of valid choices, can be given optional partial info"""
        return set()

    def __repr__(self):
        """If not overridden, will print the slots as args"""

        slots = {}
        for cls in self.__mro__:
            for prop in getattr(cls, "__slots__", ()):
                if prop[0] != "_":
                    slots[prop] = getattr(self, prop)
        mystrs = ("{} = {}".format(name, slots[name]) for name in slots)
        return "{}({})".format(self.__class__.__name__, ", ".join(mystrs))


# ===================================================================================================
# Switch type validators
# ===================================================================================================
class Range(Validator):
    """
    A switch-type validator that checks for the inclusion of a value in a certain range.
    Usage::

        class MyApp(Application):
            age = SwitchAttr(["--age"], Range(18, 120))

    :param start: The minimal value
    :param end: The maximal value
    """

    __slots__ = ("start", "end")

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __repr__(self):
        return "[{:d}..{:d}]".format(self.start, self.end)

    def __call__(self, obj):
        obj = int(obj)
        if obj < self.start or obj > self.end:
            raise ValueError(
                _("Not in range [{0:d}..{1:d}]").format(self.start, self.end)
            )
        return obj

    def choices(self, partial=""):
        # TODO: Add partial handling
        return set(range(self.start, self.end + 1))


class Set(Validator):
    """
    A switch-type validator that checks that the value is contained in a defined
    set of values. Usage::

        class MyApp(Application):
            mode = SwitchAttr(["--mode"], Set("TCP", "UDP", case_sensitive = False))
            num = SwitchAttr(["--num"], Set("MIN", "MAX", int, csv = True))

    :param values: The set of values (strings), or other callable validators, or types,
                   or any other object that can be compared to a string.
    :param case_sensitive: A keyword argument that indicates whether to use case-sensitive
                             comparison or not. The default is ``False``
    :param csv: splits the input as a comma-separated-value before validating and returning
                a list. Accepts ``True``, ``False``, or a string for the separator
    """

    def __init__(self, *values, **kwargs):
        self.case_sensitive = kwargs.pop("case_sensitive", False)
        self.csv = kwargs.pop("csv", False)
        if self.csv is True:
            self.csv = ","
        if kwargs:
            raise TypeError(
                _("got unexpected keyword argument(s): {0}").format(kwargs.keys())
            )
        self.values = values

    def __repr__(self):
        return "{{{0}}}".format(
            ", ".join(v if isinstance(v, str) else v.__name__ for v in self.values)
        )

    def __call__(self, value, check_csv=True):
        if self.csv and check_csv:
            return [self(v.strip(), check_csv=False) for v in value.split(",")]
        if not self.case_sensitive:
            value = value.lower()
        for opt in self.values:
            if isinstance(opt, str):
                if not self.case_sensitive:
                    opt = opt.lower()
                if opt == value:
                    return opt  # always return original value
                continue
            try:
                return opt(value)
            except ValueError:
                pass
        raise ValueError(
            "Invalid value: {} (Expected one of {})".format(value, self.values)
        )

    def choices(self, partial=""):
        choices = {
            opt if isinstance(opt, str) else "({})".format(opt) for opt in self.values
        }
        if partial:
            choices = {opt for opt in choices if opt.lower().startswith(partial)}
        return choices


CSV = Set(str, csv=True)


class Predicate(object):
    """A wrapper for a single-argument function with pretty printing"""

    def __init__(self, func):
        self.func = func

    def __str__(self):
        return self.func.__name__

    def __call__(self, val):
        return self.func(val)

    def choices(self, partial=""):
        return set()


@Predicate
def ExistingDirectory(val):
    """A switch-type validator that ensures that the given argument is an existing directory"""
    p = local.path(val)
    if not p.is_dir():
        raise ValueError(_("{0} is not a directory").format(val))
    return p


@Predicate
def MakeDirectory(val):
    p = local.path(val)
    if p.is_file():
        raise ValueError(
            "{} is a file, should be nonexistent, or a directory".format(val)
        )
    elif not p.exists():
        p.mkdir()
    return p


@Predicate
def ExistingFile(val):
    """A switch-type validator that ensures that the given argument is an existing file"""
    p = local.path(val)
    if not p.is_file():
        raise ValueError(_("{0} is not a file").format(val))
    return p


@Predicate
def NonexistentPath(val):
    """A switch-type validator that ensures that the given argument is a nonexistent path"""
    p = local.path(val)
    if p.exists():
        raise ValueError(_("{0} already exists").format(val))
    return p

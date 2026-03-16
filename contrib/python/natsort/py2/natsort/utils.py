# -*- coding: utf-8 -*-
"""
Utilities and definitions for natsort, mostly all used to define
the natsort_key function.

SOME CONVENTIONS USED IN THIS FILE.

1 - Factory Functions

Most of the logic of natsort revolves around factory functions
that create branchless transformation functions. For example, rather
than making a string transformation function that has an if
statement to determine whether or not to perform .lowercase() at
runtime for each element to transform, there is a string transformation
factory function that will return a function that either calls
.lowercase() or does nothing. In this way, all the branches and
decisions are taken care of once, up front. In addition to a slight
speed improvement, this provides a more extensible infrastructure.

Each of these factory functions will end with the suffix "_factory"
to indicate that they themselves return a function.

2 - Keyword Parameters For Local Scope

Many of the closures that are created by the factory functions
have signatures similar to the following

    >>> def factory(parameter):
    ...     val = 'yes' if parameter else 'no'
    ...     def closure(x, _val=val):
    ...          return '{} {}'.format(_val, x)
    ...     return closure
    ...

The variable value is passed as the default to a keyword argument.
This is a micro-optimization
that ensures "val" is a local variable instead of global variable
and thus has a slightly improved performance at runtime.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import re
from collections import deque
from functools import partial, reduce
from itertools import chain as ichain
from operator import methodcaller
from os import curdir as os_curdir
from os import pardir as os_pardir
from os import sep as os_sep
from os.path import split as path_split
from os.path import splitext as path_splitext
from unicodedata import normalize

from natsort.compat.fastnumbers import fast_float, fast_int
from natsort.compat.locale import get_decimal_point, get_strxfrm, get_thousands_sep
from natsort.compat.pathlib import PurePath, has_pathlib
from natsort.compat.py23 import (
    NEWPY,
    PY_VERSION,
    py23_filter,
    py23_map,
    py23_str,
    u_format,
)
from natsort.ns_enum import NS_DUMB, ns
from natsort.unicode_numbers import digits_no_decimals, numeric_no_decimals

if PY_VERSION >= 3:
    long = int


class NumericalRegularExpressions(object):
    """
    Container of regular expressions that match numbers.

    The numbers also account for unicode non-decimal characters.

    Not intended to be made an instance - use class methods only.
    """

    # All unicode numeric characters (minus the decimal characters).
    numeric = numeric_no_decimals
    # All unicode digit characters (minus the decimal characters).
    digits = digits_no_decimals
    # Regular expression to match exponential component of a float.
    exp = r"(?:[eE][-+]?\d+)?"
    # Regular expression to match a floating point number.
    float_num = r"(?:\d+\.?\d*|\.\d+)"

    @classmethod
    def _construct_regex(cls, fmt):
        """Given a format string, construct the regex with class attributes."""
        return re.compile(fmt.format(**vars(cls)), flags=re.U)

    @classmethod
    def int_sign(cls):
        """Regular expression to match a signed int."""
        return cls._construct_regex(r"([-+]?\d+|[{digits}])")

    @classmethod
    def int_nosign(cls):
        """Regular expression to match an unsigned int."""
        return cls._construct_regex(r"(\d+|[{digits}])")

    @classmethod
    def float_sign_exp(cls):
        """Regular expression to match a signed float with exponent."""
        return cls._construct_regex(r"([-+]?{float_num}{exp}|[{numeric}])")

    @classmethod
    def float_nosign_exp(cls):
        """Regular expression to match an unsigned float with exponent."""
        return cls._construct_regex(r"({float_num}{exp}|[{numeric}])")

    @classmethod
    def float_sign_noexp(cls):
        """Regular expression to match a signed float without exponent."""
        return cls._construct_regex(r"([-+]?{float_num}|[{numeric}])")

    @classmethod
    def float_nosign_noexp(cls):
        """Regular expression to match an unsigned float without exponent."""
        return cls._construct_regex(r"({float_num}|[{numeric}])")


def regex_chooser(alg):
    """
    Select an appropriate regex for the type of number of interest.

    Parameters
    ----------
    alg : ns enum
        Used to indicate the regular expression to select.

    Returns
    -------
    regex : compiled regex object
        Regular expression object that matches the desired number type.

    """
    if alg & ns.FLOAT:
        alg &= ns.FLOAT | ns.SIGNED | ns.NOEXP
    else:
        alg &= ns.INT | ns.SIGNED

    return {
        ns.INT: NumericalRegularExpressions.int_nosign(),
        ns.FLOAT: NumericalRegularExpressions.float_nosign_exp(),
        ns.INT | ns.SIGNED: NumericalRegularExpressions.int_sign(),
        ns.FLOAT | ns.SIGNED: NumericalRegularExpressions.float_sign_exp(),
        ns.FLOAT | ns.NOEXP: NumericalRegularExpressions.float_nosign_noexp(),
        ns.FLOAT | ns.SIGNED | ns.NOEXP: NumericalRegularExpressions.float_sign_noexp(),
    }[alg]


def _no_op(x):
    """A function that does nothing and returns the input as-is."""
    return x


def _normalize_input_factory(alg):
    """
    Create a function that will normalize unicode input data.

    Parameters
    ----------
    alg : ns enum
        Used to indicate how to normalize unicode.

    Returns
    -------
    func : callable
        A function that accepts string (unicode) input and returns the
        the input normalized with the desired normalization scheme.

    """
    normalization_form = "NFKD" if alg & ns.COMPATIBILITYNORMALIZE else "NFD"
    wrapped = partial(normalize, normalization_form)
    if NEWPY:
        return wrapped
    else:
        return lambda x, _f=wrapped: _f(x) if isinstance(x, py23_str) else x


def natsort_key(val, key, string_func, bytes_func, num_func):
    """
    Key to sort strings and numbers naturally.

    It works by splitting the string into components of strings and numbers,
    and then converting the numbers into actual ints or floats.

    Parameters
    ----------
    val : str | unicode | bytes | int | float | iterable
    key : callable | None
        A key to apply to the *val* before any other operations are performed.
    string_func : callable
        If *val* (or the output of *key* if given) is of type *str*, this
        function will be applied to it. The function must return
        a tuple.
    bytes_func : callable
        If *val* (or the output of *key* if given) is of type *bytes*, this
        function will be applied to it. The function must return
        a tuple.
    num_func : callable
        If *val* (or the output of *key* if given) is not of type *bytes*,
        *str*, nor is iterable, this function will be applied to it.
        The function must return a tuple.

    Returns
    -------
    out : tuple
        The string split into its string and numeric components.
        It *always* starts with a string, and then alternates
        between numbers and strings (unless it was applied
        recursively, in which case it will return tuples of tuples,
        but the lowest-level tuples will then *always* start with
        a string etc.).

    See Also
    --------
    parse_string_factory
    parse_bytes_factory
    parse_number_factory

    """

    # Apply key if needed
    if key is not None:
        val = key(val)

    # Assume the input are strings, which is the most common case
    try:
        return string_func(val)
    except (TypeError, AttributeError):

        # If bytes type, use the bytes_func
        if type(val) in (bytes,):
            return bytes_func(val)

        # Otherwise, assume it is an iterable that must be parsed recursively.
        # Do not apply the key recursively.
        try:
            return tuple(
                natsort_key(x, None, string_func, bytes_func, num_func) for x in val
            )

        # If that failed, it must be a number.
        except TypeError:
            return num_func(val)


def parse_bytes_factory(alg):
    """
    Create a function that will format a *bytes* object into a tuple.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format the *bytes*.

    Returns
    -------
    func : callable
        A function that accepts *bytes* input and returns a tuple
        with the formatted *bytes*. Intended to be used as the
        *bytes_func* argument to *natsort_key*.

    See Also
    --------
    natsort_key

    """
    # We don't worry about ns.UNGROUPLETTERS | ns.LOCALEALPHA because
    # bytes cannot be compared to strings.
    if alg & ns.PATH and alg & ns.IGNORECASE:
        return lambda x: ((x.lower(),),)
    elif alg & ns.PATH:
        return lambda x: ((x,),)
    elif alg & ns.IGNORECASE:
        return lambda x: (x.lower(),)
    else:
        return lambda x: (x,)


def parse_number_factory(alg, sep, pre_sep):
    """
    Create a function that will format a number into a tuple.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format the *bytes*.
    sep : str
        The string character to be inserted before the number
        in the returned tuple.
    pre_sep : str
        In the event that *alg* contains ``UNGROUPLETTERS``, this
        string will be placed in a single-element tuple at the front
        of the returned nested tuple.

    Returns
    -------
    func : callable
        A function that accepts numeric input (e.g. *int* or *float*)
        and returns a tuple containing the number with the leading string
        *sep*. Intended to be used as the *num_func* argument to
        *natsort_key*.

    See Also
    --------
    natsort_key

    """
    nan_replace = float("+inf") if alg & ns.NANLAST else float("-inf")

    def func(val, _nan_replace=nan_replace, _sep=sep):
        """Given a number, place it in a tuple with a leading null string."""
        return _sep, _nan_replace if val != val else val

    # Return the function, possibly wrapping in tuple if PATH is selected.
    if alg & ns.PATH and alg & ns.UNGROUPLETTERS and alg & ns.LOCALEALPHA:
        return lambda x: (((pre_sep,), func(x)),)
    elif alg & ns.UNGROUPLETTERS and alg & ns.LOCALEALPHA:
        return lambda x: ((pre_sep,), func(x))
    elif alg & ns.PATH:
        return lambda x: (func(x),)
    else:
        return func


def parse_string_factory(
    alg, sep, splitter, input_transform, component_transform, final_transform
):
    """
    Create a function that will split and format a *str* into a tuple.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format and split the *str*.
    sep : str
        The string character to be inserted between adjacent numeric
        objects in the returned tuple.
    splitter : callable
        A function the will accept a string and returns an iterable
        of strings where the numbers are separated from the non-numbers.
    input_transform : callable
        A function to apply to the string input *before* applying
        the *splitter* function. Must return a string.
    component_transform : callable
        A function that is operated elementwise on the output of
        *splitter*. It must accept a single string and return either
        a string or a number.
    final_transform : callable
        A function to operate on the return value as a whole. It
        must accept a tuple and a string argument - the tuple
        should be the result of applying the above functions, and the
        string is the original input value. It must return a tuple.

    Returns
    -------
    func : callable
        A function that accepts string input and returns a tuple
        containing the string split into numeric and non-numeric
        components, where the numeric components are converted into
        numeric objects. The first element is *always* a string,
        and then alternates number then string. Intended to be
        used as the *string_func* argument to *natsort_key*.

    See Also
    --------
    natsort_key
    input_string_transform_factory
    string_component_transform_factory
    final_data_transform_factory

    """
    # Sometimes we store the "original" input before transformation,
    # sometimes after.
    orig_after_xfrm = not (alg & NS_DUMB and alg & ns.LOCALEALPHA)
    original_func = input_transform if orig_after_xfrm else _no_op
    normalize_input = _normalize_input_factory(alg)

    def func(x):
        # Apply string input transformation function and return to x.
        # Original function is usually a no-op, but some algorithms require it
        # to also be the transformation function.
        x = normalize_input(x)
        x, original = input_transform(x), original_func(x)
        x = splitter(x)  # Split string into components.
        x = py23_filter(None, x)  # Remove empty strings.
        x = py23_map(component_transform, x)  # Apply transform on components.
        x = sep_inserter(x, sep)  # Insert '' between numbers.
        return final_transform(x, original)  # Apply the final transform.

    return func


def parse_path_factory(str_split):
    """
    Create a function that will properly split and format a path.

    Parameters
    ----------
    str_split : callable
        The output of the *parse_string_factory* function.

    Returns
    -------
    func : callable
        A function that accepts a string or path-like object
        and splits it into its path components, then passes
        each component to *str_split* and returns the result
        as a nested tuple. Can be used as the *string_func*
        argument to *natsort_key*.

    See Also
    --------
    natsort_key
    parse_string_factory

    """
    return lambda x: tuple(py23_map(str_split, path_splitter(x)))


def sep_inserter(iterable, sep):
    """
    Insert '' between numbers in an iterable.

    Parameters
    ----------
    iterable
    sep : str
        The string character to be inserted between adjacent numeric objects.

    Yields
    ------
    The values of *iterable* in order, with *sep* inserted where adjacent
    elements are numeric. If the first element in the input is numeric
    then *sep* will be the first value yielded.

    """
    try:
        # Get the first element. A StopIteration indicates an empty iterable.
        # Since we are controlling the types of the input, 'type' is used
        # instead of 'isinstance' for the small speed advantage it offers.
        types = (int, float, long)
        first = next(iterable)
        if type(first) in types:
            yield sep
        yield first

        # Now, check if pair of elements are both numbers. If so, add ''.
        second = next(iterable)
        if type(first) in types and type(second) in types:
            yield sep
        yield second

        # Now repeat in a loop.
        for x in iterable:
            first, second = second, x
            if type(first) in types and type(second) in types:
                yield sep
            yield second
    except StopIteration:
        # Catch StopIteration per deprecation in PEP 479:
        # "Change StopIteration handling inside generators"
        return


def input_string_transform_factory(alg):
    """
    Create a function to transform a string.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format the *str*.

    Returns
    -------
    func : callable
        A function to be used as the *input_transform* argument to
        *parse_string_factory*.

    See Also
    --------
    parse_string_factory

    """
    # Shortcuts.
    lowfirst = alg & ns.LOWERCASEFIRST
    dumb = alg & NS_DUMB

    # Build the chain of functions to execute in order.
    function_chain = []
    if (dumb and not lowfirst) or (lowfirst and not dumb):
        function_chain.append(methodcaller("swapcase"))

    if alg & ns.IGNORECASE:
        if NEWPY:
            function_chain.append(methodcaller("casefold"))
        else:
            function_chain.append(methodcaller("lower"))

    if alg & ns.LOCALENUM:
        # Create a regular expression that will remove thousands separators.
        strip_thousands = r"""
            (?<=[0-9]{{1}})  # At least 1 number
            (?<![0-9]{{4}})  # No more than 3 numbers
            {nodecimal}      # Cannot follow decimal
            {thou}           # The thousands separator
            (?=[0-9]{{3}}    # Three numbers must follow
             ([^0-9]|$)      # But a non-number after that
            )
        """
        nodecimal = r""
        if alg & ns.FLOAT:
            # Make a regular expression component that will ensure no
            # separators are removed after a decimal point.
            d = re.escape(get_decimal_point())
            nodecimal += r"(?<!" + d + r"[0-9])"
            nodecimal += r"(?<!" + d + r"[0-9]{2})"
            nodecimal += r"(?<!" + d + r"[0-9]{3})"
        strip_thousands = strip_thousands.format(
            thou=re.escape(get_thousands_sep()), nodecimal=nodecimal
        )
        strip_thousands = re.compile(strip_thousands, flags=re.VERBOSE)
        function_chain.append(partial(strip_thousands.sub, ""))

        # Create a regular expression that will change the decimal point to
        # a period if not already a period.
        decimal = get_decimal_point()
        if alg & ns.FLOAT and decimal != ".":
            switch_decimal = r"(?<=[0-9]){decimal}|{decimal}(?=[0-9])"
            switch_decimal = switch_decimal.format(decimal=re.escape(decimal))
            switch_decimal = re.compile(switch_decimal)
            function_chain.append(partial(switch_decimal.sub, "."))

    # Return the chained functions.
    return chain_functions(function_chain)


def string_component_transform_factory(alg):
    """
    Create a function to either transform a string or convert to a number.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format the *str*.

    Returns
    -------
    func : callable
        A function to be used as the *component_transform* argument to
        *parse_string_factory*.

    See Also
    --------
    parse_string_factory

    """
    # Shortcuts.
    use_locale = alg & ns.LOCALEALPHA
    dumb = alg & NS_DUMB
    group_letters = (alg & ns.GROUPLETTERS) or (use_locale and dumb)
    nan_val = float("+inf") if alg & ns.NANLAST else float("-inf")

    # Build the chain of functions to execute in order.
    func_chain = []
    if group_letters:
        func_chain.append(groupletters)
    if use_locale:
        func_chain.append(get_strxfrm())
    kwargs = {"key": chain_functions(func_chain)} if func_chain else {}

    # Return the correct chained functions.
    if alg & ns.FLOAT:
        # noinspection PyTypeChecker
        kwargs["nan"] = nan_val
        return partial(fast_float, **kwargs)
    else:
        return partial(fast_int, **kwargs)


def final_data_transform_factory(alg, sep, pre_sep):
    """
    Create a function to transform a tuple.

    Parameters
    ----------
    alg : ns enum
        Indicate how to format the *str*.
    sep : str
        Separator that was passed to *parse_string_factory*.
    pre_sep : str
        String separator to insert at the at the front
        of the return tuple in the case that the first element
        is *sep*.

    Returns
    -------
    func : callable
        A function to be used as the *final_transform* argument to
        *parse_string_factory*.

    See Also
    --------
    parse_string_factory

    """
    if alg & ns.UNGROUPLETTERS and alg & ns.LOCALEALPHA:
        swap = alg & NS_DUMB and alg & ns.LOWERCASEFIRST
        transform = methodcaller("swapcase") if swap else _no_op

        def func(split_val, val, _transform=transform, _sep=sep, _pre_sep=pre_sep):
            """
            Return a tuple with the first character of the first element
            of the return value as the first element, and the return value
            as the second element. This will be used to perform gross sorting
            by the first letter.
            """
            split_val = tuple(split_val)
            if not split_val:
                return (), ()
            elif split_val[0] == _sep:
                return (_pre_sep,), split_val
            else:
                return (_transform(val[0]),), split_val

        return func
    else:
        return lambda split_val, val: tuple(split_val)


lower_function = methodcaller("casefold" if NEWPY else "lower")


# noinspection PyIncorrectDocstring
@u_format
def groupletters(x, _low=lower_function):
    """
    Double all characters, making doubled letters lowercase.

    Parameters
    ----------
    x : str

    Returns
    -------
    str

    Examples
    --------

        >>> groupletters("Apple")
        {u}'aAppppllee'

    """
    return "".join(ichain.from_iterable((_low(y), y) for y in x))


def chain_functions(functions):
    """
    Chain a list of single-argument functions together and return.

    The functions are applied in list order, and the output of the
    previous functions is passed to the next function.

    Parameters
    ----------
    functions : list
        A list of single-argument functions to chain together.

    Returns
    -------
    func : callable
        A single argument function.

    Examples
    --------
    Chain several functions together!

        >>> funcs = [lambda x: x * 4, len, lambda x: x + 5]
        >>> func = chain_functions(funcs)
        >>> func('hey')
        17

    """
    functions = list(functions)
    if not functions:
        return _no_op
    elif len(functions) == 1:
        return functions[0]
    else:
        # See https://stackoverflow.com/a/39123400/1399279
        return partial(reduce, lambda res, f: f(res), functions)


def do_decoding(s, encoding):
    """
    Helper to decode a *bytes* object, or return the object as-is.

    Parameters
    ----------
    s : bytes | object
    encoding : str
        The encoding to use to decode *s*.

    Returns
    -------
    decoded
        *str* if *s* was *bytes* and the decoding was successful.
        *s* if *s* was not *bytes*.

    """
    try:
        return s.decode(encoding)
    except (AttributeError, TypeError):
        return s


# noinspection PyIncorrectDocstring
@u_format
def path_splitter(s, _d_match=re.compile(r"\.\d").match):
    """
    Split a string into its path components.

    Assumes a string is a path or is path-like.

    Parameters
    ----------
    s : str | pathlib.Path

    Returns
    -------
    split : tuple
        The path split by directory components and extensions.

    Examples
    --------

        >>> tuple(path_splitter("this/thing.ext"))
        ({u}'this', {u}'thing', {u}'.ext')

    """
    if has_pathlib and isinstance(s, PurePath):
        s = py23_str(s)
    path_parts = deque()
    p_appendleft = path_parts.appendleft
    # Continue splitting the path from the back until we have reached
    # '..' or '.', or until there is nothing left to split.
    path_location = s
    while path_location != os_curdir and path_location != os_pardir:
        parent_path = path_location
        path_location, child_path = path_split(parent_path)
        if path_location == parent_path:
            break
        p_appendleft(child_path)

    # This last append is the base path.
    # Only append if the string is non-empty.
    # Make sure the proper path separator for this OS is used
    # no matter what was actually given.
    if path_location:
        p_appendleft(py23_str(os_sep))

    # Now, split off the file extensions using a similar method to above.
    # Continue splitting off file extensions until we reach a decimal number
    # or there are no more extensions.
    # We are not using built-in functionality of PathLib here because of
    # the recursive splitting up to a decimal.
    base = path_parts.pop()
    base_parts = deque()
    b_appendleft = base_parts.appendleft
    while True:
        front = base
        base, ext = path_splitext(front)
        if _d_match(ext) or not ext:
            # Reset base to before the split if the split is invalid.
            base = front
            break
        b_appendleft(ext)
    b_appendleft(base)

    # Return the split parent paths and then the split basename.
    return ichain(path_parts, base_parts)

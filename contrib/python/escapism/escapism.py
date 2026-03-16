"""Escape strings to a safe character set

Provide a generic API, given a set of safe characters and an escape character,
to escape safe strings and unescape the result.

A conservative default of [A-Za-z0-9] with _ as the escape character is used if
no args are provided.


Provides two APIs:

- `[un]escape` for lossless escape/unescape of strings
- `safe_slug` which:
  - satisfies stricter rules (e.g. Kubernetes names and labels)
  - accepts more values without transformation
  - guarantees uniqueness by appending a hash after stripping invalid characters

  But `safe_slug` is a one-way conversion; it does not preserve the original string contents in a recoverable way.
"""

# Copyright (c) Min RK
# Distributed under the terms of the MIT License

import hashlib
import re
import string
import warnings

__version__ = "1.1.0"

SAFE = set(string.ascii_letters + string.digits)
ESCAPE_CHAR = "_"

_ord = lambda byte: byte
_bchr = lambda n: bytes([n])


def _escape_char(c, escape_char=ESCAPE_CHAR):
    """Escape a single character"""
    buf = []
    for byte in c.encode("utf8"):
        buf.append(escape_char)
        buf.append(f"{_ord(byte):X}")
    return "".join(buf)


def escape(to_escape, safe=SAFE, escape_char=ESCAPE_CHAR, allow_collisions=False):
    """Escape a string so that it only contains characters in a safe set.

    Characters outside the safe list will be escaped with _%x_,
    where %x is the hex value of the character.

    If `allow_collisions` is True, occurrences of `escape_char`
    in the input will not be escaped.

    In this case, `unescape` cannot be used to reverse the transform
    because occurrences of the escape char in the resulting string are ambiguous.
    Only use this mode when:

    1. collisions cannot occur or do not matter, and
    2. unescape will never be called.

    .. versionadded: 1.0
        allow_collisions argument.
        Prior to 1.0, behavior was the same as allow_collisions=False (default).

    """
    if isinstance(to_escape, bytes):
        # always work on text
        to_escape = to_escape.decode("utf8")

    if not isinstance(safe, set):
        safe = set(safe)

    if allow_collisions:
        safe.add(escape_char)
    elif escape_char in safe:
        warnings.warn(
            f"Escape character {escape_char!r} cannot be a safe character."
            " Set allow_collisions=True if you want to allow ambiguous escaped strings.",
            RuntimeWarning,
            stacklevel=2,
        )
        safe.remove(escape_char)

    chars = []
    for c in to_escape:
        if c in safe:
            chars.append(c)
        else:
            chars.append(_escape_char(c, escape_char))
    return "".join(chars)


def _unescape_char(m):
    """Unescape a single byte

    Used as a callback in pattern.subn. `m.group(1)` must be a single byte in hex,
    e.g. `a4` or `ff`.
    """
    return _bchr(int(m.group(1), 16))


def unescape(escaped, escape_char=ESCAPE_CHAR):
    """Unescape a string escaped with `escape`

    escape_char must be the same as that used in the call to escape.
    """
    if isinstance(escaped, bytes):
        # always work on text
        escaped = escaped.decode("utf8")

    escape_pat = re.compile(
        re.escape(escape_char).encode("utf8") + b"([a-z0-9]{2})", re.IGNORECASE
    )
    buf = escape_pat.subn(_unescape_char, escaped.encode("utf8"))[0]
    return buf.decode("utf8")


# slugs

_alphanum = tuple(string.ascii_letters + string.digits)
_alpha_lower = tuple(string.ascii_lowercase)
_alphanum_lower = tuple(string.ascii_lowercase + string.digits)
_lower_plus_hyphen = _alphanum_lower + ("-",)

# patterns _do not_ need to cover length or start/end conditions,
# which are handled separately
_object_pattern = re.compile(r"^[a-z0-9\-]+$")
_label_pattern = re.compile(r"^[a-z0-9\.\-_]+$", flags=re.IGNORECASE)

# match anything that's not lowercase alphanumeric (will be stripped, replaced with '-')
_non_alphanum_pattern = re.compile(r"[^a-z0-9]+")

# length of hash suffix
_hash_length = 8

# Make sure username and servername match the restrictions for DNS labels
# Note: '-' is not in safe_chars, as it is being used as escape character
_escape_slug_safe_chars = set(string.ascii_lowercase + string.digits)


def _is_valid_general(
    s, starts_with=None, ends_with=None, pattern=None, min_length=None, max_length=None
):
    """General is_valid check

    Checks rules:
    """
    if min_length and len(s) < min_length:
        return False
    if max_length and len(s) > max_length:
        return False
    if starts_with and not s.startswith(starts_with):
        return False
    if ends_with and not s.endswith(ends_with):
        return False
    if pattern and not pattern.match(s):
        return False
    return True


def is_valid_object_name(s):
    """is_valid check for Kubernetes object names

    Ensures all strictest object rules apply,
    satisfying both RFC 1035 and 1123 dns label name rules

    - 63 characters
    - starts with letter, ends with letter or number
    - only lowercalse letters, numbers, '-'
    """
    # object rules: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
    return _is_valid_general(
        s,
        starts_with=_alpha_lower,
        ends_with=_alphanum_lower,
        pattern=_object_pattern,
        max_length=63,
        min_length=1,
    )


def is_valid_label(s):
    """is_valid check for Kubernetes label values"""
    # label rules: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
    if not s:
        # empty strings are valid labels
        return True
    return _is_valid_general(
        s,
        starts_with=_alphanum,
        ends_with=_alphanum,
        pattern=_label_pattern,
        max_length=63,
    )


def is_valid_default(s):
    """Strict is_valid

    Returns True if it's valid for _all_ our known uses

    Currently, this is the same as is_valid_object_name,
    which produces a valid DNS label under RFC1035 AND RFC 1123,
    which is always also a valid label value.
    """
    return is_valid_object_name(s)


def _extract_safe_name(name, max_length):
    """Generate safe substring of a name

    Guarantees:

    - always starts with a lowercase letter
    - always ends with a lowercase letter or number
    - never more than one hyphen in a row (no '--')
    - only contains lowercase letters, numbers, and hyphens
    - length at least 1 ('x' if other rules strips down to empty string)
    - max length not exceeded
    """
    # compute safe slug from name (don't worry about collisions, hash handles that)
    # cast to lowercase
    # replace any sequence of non-alphanumeric characters with a single '-'
    safe_name = _non_alphanum_pattern.sub("-", name.lower())
    # truncate to max_length chars, strip '-' off ends
    safe_name = safe_name.lstrip("-")[:max_length].rstrip("-")
    # ensure starts with lowercase letter
    if safe_name and not safe_name.startswith(_alpha_lower):
        safe_name = "x-" + safe_name[: max_length - 2]
    if not safe_name:
        # make sure it's non-empty
        safe_name = "x"
    return safe_name


def _strip_and_hash(name, max_length=32):
    """Generate an always-safe, unique string for any input

    truncates name to max_length - len(hash_suffix) to fit in max_length
    after adding hash suffix
    """
    name_length = max_length - (_hash_length + 3)
    if name_length < 1:
        raise ValueError(f"Cannot make safe names shorter than {_hash_length + 4}")
    # quick, short hash to avoid name collisions
    name_hash = hashlib.sha256(name.encode("utf8")).hexdigest()[:_hash_length]
    safe_name = _extract_safe_name(name, name_length)
    # due to stripping of '-' in _extract_safe_name,
    # the result will always have _exactly_ '---', never '--' nor '----'
    # use '---' to avoid colliding with `{username}--{servername}` template join
    return f"{safe_name}---{name_hash}"


def safe_slug(name, *, is_valid=is_valid_default, max_length=None):
    """Always generate a safe slug

    is_valid should be a callable that returns True if a given string follows appropriate rules,
    and False if it does not.

    Given a string, if it's already valid, use it.
    If it's not valid, follow a safe encoding scheme that ensures:

    1. validity, and
    2. no collisions
    """
    if "--" in name:
        # don't accept any names that could collide with the safe slug
        return _strip_and_hash(name, max_length=max_length or 32)
    # allow max_length override for truncated sub-strings
    if is_valid(name) and (max_length is None or len(name) <= max_length):
        return name
    else:
        return _strip_and_hash(name, max_length=max_length or 32)


def multi_slug(names, max_length=48):
    """multi-component slug with single hash on the end

    same as `strip_and_hash`, but name components are joined with '--',
    so it looks like:

    {name1}--{name2}---{hash}

    In order to avoid hash collisions on boundaries, use `\\xFF` as delimiter
    """
    hasher = hashlib.sha256()
    hasher.update(names[0].encode("utf8"))
    for name in names[1:]:
        # \xFF can't occur as a start byte in UTF8
        # so use it as a word delimiter to make sure overlapping words don't collide
        hasher.update(b"\xff")
        hasher.update(name.encode("utf8"))
    hash = hasher.hexdigest()[:_hash_length]

    name_slugs = []
    available_chars = max_length - (_hash_length + 1)
    # allocate equal space per name
    # per_name accounts for '{name}--', so really two less
    per_name = available_chars // len(names)
    name_max_length = per_name - 2
    if name_max_length < 2:
        raise ValueError(f"Not enough characters for {len(names)} names: {max_length}")
    for name in names:
        name_slugs.append(_extract_safe_name(name, name_max_length))

    # by joining names with '--', this cannot collide with single-hashed names,
    # which can only contain '-' and the '---' hash delimiter once
    return f"{'--'.join(name_slugs)}---{hash}"

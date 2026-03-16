# Escapism

Simple escaping of text, given a set of safe characters and an escape character.

## Usage

Not much to it. Two functions:

```python
escaped = escapism.escape('string to escape')
# 'string_20to_20escape'
original = escapism.unescape(escaped)
```

There are two optional arguments you can pass to `escape()`:

- `safe`: a string or set of characters that don't need escaping. Default: ascii letters and numbers.
- `escape_char`: a single character used for escaping. Default: `_`.
  `escape_char` will never be considered a safe value.

`unescape()` accepts the same `escape_char` argument as `escape()` if a value other than the default is used.

```python
import string
import escapism
safe = string.ascii_letters + string.digits + '@_-.+'
escape_char = r'%'
escaped = escapism.escape('foø-bar@%!xX?', safe=safe, escape_char=escape_char)
# 'fo%C3%B8-bar@%25%21xX%3F'
original = escapism.unescape(escaped, escape_char=escape_char)
```

### Slugs

escapism 1.1 adds a `safe_slug` API (extracted from kubespawner 7).

When to use `safe_slug` instead of `escape`:

- you don't need to recover the original string (`safe_slug` is lossy), but you do need:
- length requirements
- start/end rules
- arbitrary other validity requirements
- uniqueness

`safe_slug` takes an `is_valid` callable,
which you can use to specify whether a key passes a validity check.
If it passes, the string is returned unmodified.

The default `is_valid` callable applies a strict subset of various kubernetes rules,
so it will always return a unique string that is valid in just about any kubernetes field (object name, label values, etc.)

- min length of 1
- max length of 63
- contains only lowercase ascii letters, numbers, and '-'
- starts with a letter
- ends with a letter or number

If the input string does not pass the `is_valid` check,
it is stripped and hashed to ensure validity and uniqueness.

This does:

- cast to lowercase
- strip all non-alphanumeric characters
- if it doesn't start with a letter, prefix with `x-`
- truncate the safe subset
- append a hash of the original name, utf8-encoded, after `---`

Examples:

```python
safe_slug("valid-slug") # "valid-slug"
safe_slug("4start") # "x-4start---3a570dbd"
safe_slug("a" * 64, max_length=20) # "aaaaaaaaa---ffe054fe"
safe_slug("üñîçø∂é") # "x---4072f5a6"
```

Note: the 'safe' result is not customizable,
which means that `safe_slug` can only be used if this trim-and-hash result is valid in your scheme.

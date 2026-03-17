# greenery

Tools for parsing and manipulating regular expressions. Note that this is a very different concept from that of simply *creating and using* those regular expressions, functionality which is present in basically every programming language in the world, [Python included](http://docs.python.org/library/re.html).

This project was undertaken because I wanted to be able to **compute the intersection between two regular expressions**. The "intersection" is the set of strings which both regular expressions will accept, represented as a third regular expression.

## Installation

```sh
pip install greenery
```

## Example

```python
from greenery import parse

print(parse("abc...") & parse("...def"))
# "abcdef"

print(parse("\d{4}-\d{2}-\d{2}") & parse("19.*"))
# "19\d{2}-\d{2}-\d{2}"

print(parse("\W*") & parse("[a-g0-8$%\^]+") & parse("[^d]{2,8}"))
# "[$%\^]{2,8}"

print(parse("[bc]*[ab]*") & parse("[ab]*[bc]*"))
# "([ab]*a|[bc]*c)?b*"

print(parse("a*") & parse("b*"))
# ""

print(parse("a") & parse("b"))
# "[]"
```

In the penultimate example, the empty string is returned, because only the empty string is in both of the regular languages `a*` and `b*`. In the final example, an empty character class has been returned. An empty character class can never match anything, which means `greenery` can use this to represent a regular expression which matches no strings at all. Note that this is different from only matching the empty string.

Internally, `greenery` works by converting regular expressions to finite state machines, computing the intersection of the two FSMs as a third FSM, and using the Brzozowski algebraic method (*q.v.*) to convert the third FSM back to a regular expression.

## API

### parse(string)

This function takes a regular expression (_i.e._ a string) as input and returns a `Pattern` object (see below) representing that regular expression.

The following metacharacters and formations have their usual meanings: `.`, `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`, `()`, `|`, `[]`, `^` within `[]` character ranges only, `-` within `[]` character ranges only, and `\` to escape any of the preceding characters or itself.

These character escapes are possible: `\t`, `\r`, `\n`, `\f`, `\v`.

These predefined character sets also have their usual meanings: `\w`, `\d`, `\s` and their negations `\W`, `\D`, `\S`. `.` matches any character, including new line characters and carriage returns.

An empty charclass `[]` is legal and matches no characters: when used in a regular expression, the regular expression may match no strings.

#### Unsupported constructs

* This method is intentionally rigorously simple, and tolerates no ambiguity. For example, a hyphen must be escaped in a character class even if it appears first or last. `[-abc]` is a syntax error, write `[\-abc]`. Escaping something which doesn't need it is a syntax error too: `[\ab]` resolves to neither `[\\ab]` nor `[ab]`.

* The `^` and `$` metacharacters are not supported. By default, `greenery` assumes that all regexes are anchored at the start and end of any input string. Carets and dollar signs will be parsed as themselves. If you want to *not* anchor at the start or end of the string, put `.*` at the start or end of your regex respectively.

  This is because computing the intersection between `.*a.*` and `.*b.*` (1) is largely pointless and (2) usually results in gibberish coming out of the program.

* The non-greedy operators `*?`, `+?`, `??` and `{m,n}?` are permitted but do nothing. This is because they do not alter the regular language. For example, `abc{0,5}def` and `abc{0,5}?def` represent precisely the same set of strings.

* Parentheses are used to alternate between multiple possibilities e.g. `(a|bc)` only, not for capture grouping. Here's why:

  ```python
  print(parse("(ab)c") & parse("a(bc)"))
  # "abc"
  ```

* The `(?:...)` syntax for non-capturing groups is permitted, but does nothing.

* Other `(?...)` constructs are not supported (and most are not [regular in the computer science sense](http://en.wikipedia.org/wiki/Regular_language)).

*  Back-references, such as `([aeiou])\1`, are not regular.

### Pattern

A `Pattern` represents a regular expression and exposes various methods for manipulating it and combining it with other regular expressions. `Pattern`s are immutable.

A regular language is a possibly-infinite set of strings. With this in mind, `Pattern` implements numerous [methods like those on `frozenset`](https://docs.python.org/3/library/stdtypes.html#frozenset), as well as many regular expression-specific methods.

It's not intended that you construct new `Pattern` instances directly; use `parse(string)`, above.

Method | Behaviour
---|---
`pattern.matches("a")` <br/> `"a" in pattern` | Returns `True` if the regular expression matches the string or `False` if not.
`pattern.strings()` <br/> `for string in pattern` | Returns a generator of all the strings that this regular expression matches.
`pattern.empty()` | Returns `True` if this regular expression matches no strings, otherwise `False`.
`pattern.cardinality()` <br/> `len(pattern)` | Returns the number of strings which the regular expression matches. Throws an `OverflowError` if this number is infinite.
`pattern1.equivalent(pattern2)` | Returns `True` if the two regular expressions match exactly the same strings, otherwise `False`.
`pattern.copy()` | Returns a shallow copy of `pattern`.
`pattern.everythingbut()` | Returns a regular expression which matches every string not matched by the original. `pattern.everythingbut().everythingbut()` matches the same strings as `pattern`, but is not necessarily identical in structure.
`pattern.reversed()` <br/> `reversed(pattern)` | Returns a reversed regular expression. For each string that `pattern` matched, `reversed(pattern)` will match the reversed string. `reversed(reversed(pattern))` matches the same strings as `pattern`, but is not necessarily identical.
`pattern.times(star)` <br/> `pattern * star` | Returns the input regular expression multiplied by any `Multiplier` (see below).
`pattern1.concatenate(pattern2, ...)` <br/> `pattern1 + pattern2 + ...` | Returns a regular expression which matches any string of the form *a·b·...* where *a* is a string matched by `pattern1`, *b* is a string matched by `pattern2` and so on.
`pattern1.union(pattern2, ...)` <br/> `pattern1 \| pattern2 \| ...` | Returns a regular expression matching any string matched by any of the input regular expressions. This is also called *alternation*.
`pattern1.intersection(pattern2, ...)` <br/> `pattern1 & pattern2 & ...` | Returns a regular expression matching any string matched by all input regular expressions. The successful implementation of this method was the ultimate goal of this entire project.
`pattern1.difference(pattern2, ...)` <br/> `pattern1 - pattern2 - ...` | Subtract the set of strings matched by `pattern2` onwards from those matched by `pattern1` and return the resulting regular expression.
`pattern1.symmetric_difference(pattern2, ...)` <br/> `pattern1 ^ pattern2 ^ ...` | Returns a regular expression matching any string accepted by `pattern1` or `pattern2` but not both.
`pattern.derive("a")` | Return the [Brzozowski derivative](https://en.wikipedia.org/wiki/Brzozowski_derivative) of the input regular expression with respect to "a".
`pattern.reduce()` | Returns a regular expression which is equivalent to `pattern` (*i.e.* matches exactly the same strings) but is simplified as far as possible. See dedicated section below.

#### pattern.reduce()

Call this method to try to simplify the regular expression object. The follow simplification heuristics are supported:

* `(ab|cd|ef|)g` to `(ab|cd|ef)?g`
* `([ab])*` to `[ab]*`
* `ab?b?c` to `ab{0,2}c`
* `aa` to `a{2}`
* `a(d(ab|a*c))` to `ad(ab|a*c)`
* `0|[2-9]` to `[02-9]`
* `abc|ade` to `a(bc|de)`
* `xyz|stz` to `(xy|st)z`
* `abc()def` to `abcdef`
* `a{1,2}|a{3,4}` to `a{1,4}`

The value returned is a new `Pattern` object.

Note that in a few cases this did *not* result in a shorter regular expression.

### Multiplier

A combination of a finite lower `Bound` (see below) and a possibly-infinite upper `Bound`.

```python
from greenery import parse, Bound, INF, Multiplier

print(parse("a") * Multiplier(Bound(3), INF)) # "a{3,}"
```

### STAR

Special `Multiplier`, equal to `Multiplier(Bound(0), INF)`. When it appears in a regular expression, this is `{0,}` or the [Kleene star](https://en.wikipedia.org/wiki/Kleene_star) `*`.

### QM

Special `Multiplier`, equal to `Multiplier(Bound(0), Bound(1))`. When it appears in a regular expression, this is `{0,1}` or `?`.

### PLUS

Special `Multiplier`, equal to `Multiplier(Bound(1), INF)`. When it appears in a regular expression, this is `{1,}` or `+`.

### Bound

Represents a non-negative integer or infinity.

### INF

Special `Bound` representing no limit. Can be used as an upper bound only.

### Charclass

This class represents a _character class_ such as `a`, `\w`, `.`, `[A-Za-z0-9_]`, and so on. `Charclass`es must be constructed longhand either using a string containing all the desired characters, or a tuple of ranges, where each range is a pair of characters to be used as the range's inclusive endpoints. Use `~` to negate a `Charclass`.

* `a` = `Charclass("a")`
* `[abyz]` = `Charclass("abyz")`
* `[a-z]` = `Charclass("abcdefghijklmnopqrstuvwxyz")` or `Charclass((("a", "z"),))`
* `\w` = `Charclass((("a", "z"), ("A", "Z"), ("0", "9"), ("_", "_")))`
* `[^x]` = `~Charclass("x")`
* `\D` = `~Charclass("0123456789")`
* `.` = `~Charclass(())`

### Fsm

An `Fsm` is a finite state machine which accepts strings (or more generally iterables of Unicode characters) as input. This is used internally by `Pattern` for most regular expression manipulation operations.

In theory, accepting strings as input means that every `Fsm`'s alphabet is the same: the set of all 1,114,112 possible Unicode characters which can make up a string. But this is a very large alphabet and would result in extremely large transition maps, and have very poor performance. So, in practice, `Fsm` uses not single characters but `Charclass`es (see above) for its alphabet and its map transitions.

```python
# FSM accepting only the string "a"
a = Fsm(
    alphabet={Charclass("a"), ~Charclass("a")},
    states={0, 1, 2},
    initial=0,
    finals={1},
    map={
        0: {Charclass("a"): 1, ~Charclass("a"): 2},
        1: {Charclass("a"): 2, ~Charclass("a"): 2},
        2: {Charclass("a"): 2, ~Charclass("a"): 2},
    },
)
```

Notes:

* The `Charclass`es which make up the alphabet must _partition_ the space of all Unicode characters - every Unicode character must be a member of exactly one `Charclass` in the alphabet.
* States must be integers.
* The map must be complete. Omitting transition symbols or states is not permitted.

A regular language is a possibly-infinite set of strings. With this in mind, `Fsm` implements several [methods like those on `frozenset`](https://docs.python.org/3/library/stdtypes.html#frozenset).

Method | Behaviour
---|---
`fsm.accepts("a")` | Returns `True` if the FSM accepts string or `False` if not.
`fsm.strings()` | Returns a generator of all the strings which this FSM accepts.
`fsm.empty()` | Returns `True` if this FSM accepts no strings, otherwise `False`.
`fsm.cardinality()` | Returns the number of strings which the FSM accepts. Throws an `OverflowError` if this number is infinite.
`fsm1.equivalent(fsm2)` | Returns `True` if the two FSMs accept exactly the same strings, otherwise `False`.
`fsm.copy()` | Returns a shallow copy of `fsm`.
`fsm.everythingbut()` | Returns an FSM which accepts every string not matched by the original. `fsm.everythingbut().everythingbut()` matches the same strings as `fsm`.
`fsm1.concatenate(fsm2, ...)` | Returns an FSM which accepts any string of the form *a·b·...* where *a* is a string accepted by `fsm1`, *b* is a string accepted by `fsm2` and so on.
`fsm.times(multiplier)` | Returns the input FSM concatenated with itself `multiplier` times. `multiplier` must be a non-negative integer.
`fsm.star()` | Returns an FSM which is the Kleene star closure of the original.
`fsm1.union(fsm2, ...)` | Returns an FSM accepting any string matched by any of the input FSMs. This is also called *alternation*.
`fsm1.intersection(fsm2, ...)` | Returns an FSM accepting any string matched by all input FSMs.
`fsm1.difference(fsm2, ...)` | Subtract the set of strings matched by `fsm2` onwards from those matched by `fsm1` and return the resulting FSM.
`fsm1.symmetric_difference(fsm2, ...)` | Returns an FSM matching any string accepted by `fsm1` or `fsm2` but not both.
`fsm.derive(string)` | Return the [Brzozowski derivative](https://en.wikipedia.org/wiki/Brzozowski_derivative) of the input FSM with respect to the input string.
`fsm.reduce()` | Returns an FSM which is equivalent to `fsm` (*i.e.* accepts exactly the same strings) but has a minimal number of states.

Note that methods combining FSMs usually output new FSMs with modified alphabets. For example, concatenating an FSM with alphabet `{Charclass("a"), ~Charclass("a")}` and another FSM with alphabet `{Charclass("abc"), ~Charclass("abc")}` usually results in a third FSM with a _repartitioned_ alphabet of `{Charclass("a"), Charclass("bc"), ~Charclass("abc")}`. Notice how all three alphabets partition the space of all Unicode characters.

Several other methods on `Fsm` instances are available - these should not be used, they're subject to change.

### EPSILON

Special `Fsm` which accepts only the empty string.

### NULL

Special `Fsm` which accepts no strings.

## Development

### Running tests

```sh
pip install -r requirements.dev.txt
isort .
black .
mypy greenery
flake8 --count --statistics --show-source --select=E9,F63,F7,F82 .
flake8 --count --statistics --exit-zero --max-complexity=10 .
pylint --recursive=true .
pytest
```

### Building and publishing new versions

* Update the version in `./setup.py`
* Trash `./dist`
* `python -m build` - creates a `./dist` directory with some stuff in it
* `python -m twine upload dist/*`

# More Parsing!

[![PyPI Latest Release](https://img.shields.io/pypi/v/mo-parsing.svg)](https://pypi.org/project/mo-parsing/)
 [![Build Status](https://github.com/klahnakoski/mo-parsing/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/klahnakoski/mo-parsing/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/github/klahnakoski/mo-parsing/badge.svg?branch=master)](https://coveralls.io/github/klahnakoski/mo-parsing?branch=master)

A fork of [pyparsing](https://github.com/pyparsing/pyparsing) for faster parsing


## Installation

This is a pypi package

    pip install mo-parsing
    
## Usage

This module allows you to define a PEG parser using predefined patterns and Python operators.  Here is an example 

```
>>> from mo_parsing import Word
>>> from mo_parsing.utils import alphas
>>>
>>> greet = Word(alphas)("greeting") + "," + Word(alphas)("person") + "!"
>>> result = greet.parse_string("Hello, World!")
```

The `result` can be accessed as a nested list

```
>>> list(result)
['Hello', ',', 'World', '!']
```

The `result` can also be accessed as a dictionary

```
>>> dict(result)
{'greeting': 'Hello', 'person': 'World'}
```

Read the [pyparsing documentation](https://github.com/pyparsing/pyparsing/#readme) for more

### The `Whitespace` Context

The `mo_parsing.whitespaces.CURRENT` is used during parser creation: It is effectively defines what "whitespace" to skip during parsing, with additional features to simplify the language definition.  You declare "standard" `Whitespace` like so:

    with Whitespace() as whitespace:
        # PUT YOUR LANGUAGE DEFINITION HERE (space, tab and CR are "whitespace")

If you are declaring a large language, and you want to minimize indentation, and you are careful, you may also use this pattern:

    whitespace = Whitespace().use()
    # PUT YOUR LANGUAGE DEFINITION HERE
    whitespace.release()

The whitespace can be used to set global parsing parameters, like

* `set_whitespace()` - set the ignored characters (default: `"\t\n "`)
* `add_ignore()` - include whole patterns that are ignored (like comments)
* `set_literal()` - Set the definition for what `Literal()` means
* `set_keyword_chars()` - For default `Keyword()` (important for defining word boundary)


### Navigating ParseResults

The results of parsing are in `ParseResults` and are in the form of an n-ary tree; with the children found in `ParseResults.tokens`.  Each `ParseResult.type` points to the `ParserElement` that made it.  In general, if you want to get fancy with post processing (or in a `parse_action`), you will be required to navigate the raw `tokens` to generate a final result

There are some convenience methods;  
* `__iter__()` - allows you to iterate through parse results in **depth first search**. Empty results are skipped, and `Group`ed results are treated as atoms (which can be further iterated if required) 
* `name` is a convenient property for `ParseResults.type.token_name`
* `__getitem__()` - allows you to jump into the parse tree to the given `name`. This is blocked by any names found inside `Group`ed results (because groups are considered atoms).      

### Parse Actions

Parse actions are methods that run after a ParserElement found a match. 

* Parameters must be accepted in `(tokens, index, string)` order (the opposite of pyparsing)
* Parse actions are wrapped to ensure the output is a legitimate ParseResult
  * If your parse action returns `None` then the result is the original `tokens`
  * If your parse action returns an object, or list, or tuple, then it will be packaged in a `ParseResult` with same type as `tokens`.
  * If your parse action returns a `ParseResult` then it is accepted ***even if is belongs to some other pattern***
  
#### Simple example:

```
integer = Word("0123456789").add_parse_action(lambda t, i, s: int(t[0]))
result = integer.parse_string("42")
assert (result[0] == 42)
```

For slightly shorter specification, you may use the `/` operator and only parameters you need:

```
integer = Word("0123456789") / (lambda t: int(t[0]))
result = integer.parse_string("42")
assert (result[0] == 42)
```

### Debugging

The PEG-style of mo-parsing (from pyparsing) makes a very expressible and readable specification, but debugging a parser is still hard.  To look deeper into what the parser is doing use the `Debugger`:

```
with Debugger():
    expr.parse_string("my new language")
```

The debugger will print out details of what's happening

* Each attempt, and if it matched or failed
* A small number of bytes to show you the current position
* location, line and column for more info about the current position
* whitespace indicating stack depth
* print out of the ParserElement performing the attempt

This should help to isolate the exact position your grammar is failing. 

### Regular Expressions

`mo-parsing` can parse and generate regular expressions. `ParserElement` has a `__regex__()` function that returns the regular expression for the given grammar; which works up to a limit, and is used internally to accelerate parsing.  The `Regex` class parses regular expressions into a grammar; it is used to optimize parsing, and you may find it useful to decompose regular expressions that look like line noise.


## Differences from PyParsing

This fork was originally created to support faster parsing for [mo-sql-parsing](https://github.com/klahnakoski/moz-sql-parser).  Since then it has deviated sufficiently to be it's own collection of parser specification functions.  Here are the differences:

* Added `Whitespace`, which controls parsing context and whitespace.  It replaces the whitespace modifying methods of pyparsing
* the wildcard ("`*`") could be used in pyparsing to indicate multi-values are expected; this is not allowed in `mo-parsing`: all values are multi-values
* ParserElements are static: For example, `expr.add_parse_action(action)` creates a new ParserElement, so must be assigned to variable or it is lost. **This is the biggest source of bugs when converting from pyparsing**
* removed all backward-compatibility settings
* no support for binary serialization (no pickle)

Faster Parsing

* faster infix operator parsing (main reason for this fork)
* ParseResults point to ParserElement for reduced size
* regex used to reduce the number of failed parse attempts  
* packrat parser is not need
* less stack used 



## Contributing

If you plan to extend or enhance this code, please [see the README in the tests directory](https://github.com/klahnakoski/mo-parsing/blob/dev/tests/README.md)
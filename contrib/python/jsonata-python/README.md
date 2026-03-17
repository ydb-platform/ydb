# jsonata-python

[![Build Status][github-actions-shield]][github-actions-link]
[![PyPI](https://img.shields.io/pypi/v/jsonata-python.svg)](https://www.pypi.org/project/jsonata-python)

[github-actions-shield]: https://github.com/rayokota/jsonata-python/actions/workflows/test.yml/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/jsonata-python/actions

Pure Python implementation of JSONata.

This is a Python port of the  [JSONata reference implementation](https://github.com/jsonata-js/jsonata), 
and also borrows from the [Dashjoin Java port](https://github.com/dashjoin/jsonata-java).

This implementation supports 100% of the language features of JSONata, with no external dependencies.
The JSONata documentation can be found [here](https://jsonata.org).


## Installation

```
pipx install jsonata-python
```

## Getting Started

A very simple start:

```
>>> import jsonata
>>> data = {"example": [{"value": 4}, {"value": 7}, {"value": 13}]}
>>> expr = jsonata.Jsonata("$sum(example.value)")
>>> result = expr.evaluate(data)
>>> result
24
```

## Command Line Interface

The CLI provides the same functionality as the [Dashjoin JSONata CLI](https://github.com/dashjoin/jsonata-cli).

```
% jsonata -h
usage: jsonata [-h] [-v] [-e <file>] [-i <arg>] [-ic <arg>] [-f {auto,json,string}] [-o <arg>] [-oc <arg>] [-time] [-c] [-b <json-string>]
                   [-bf <file>] [-it]
                   [expr]

Pure Python JSONata CLI

positional arguments:
  expr

options:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -e <file>, --expression <file>
                        JSON expression to evaluate.
  -i <arg>, --input <arg>
                        JSON input file (- for stdin)
  -ic <arg>, --icharset <arg>
                        Input character set (default=utf-8)
  -f {auto,json,string}, --format {auto,json,string}
                        Input format (default=auto)
  -o <arg>, --output <arg>
                        JSON output file (default=stdout)
  -oc <arg>, --ocharset <arg>
                        Output character set (default=utf-8)
  -time                 Print performance timers to stderr
  -c, --compact         Compact JSON output (don't prettify)
  -b <json-string>, --bindings <json-string>
                        JSONata variable bindings
  -bf <file>, --bindings-file <file>
                        JSONata variable bindings file
  -it, --interactive    Interactive REPL (requires input file)
```

### Examples

```
% echo '{"a":"hello", "b":" world"}' | jsonata '(a & b)'
hello world

% echo '{"a":"hello", "b":" world"}' | jsonata -o helloworld.json $
# helloworld.json written

% ls | jsonata $
helloworld.json

% ps -o pid="",%cpu="",%mem="" | jsonata '$.$split(/\n/).$trim().[ $split(/\s+/)[$length()>0].$number() ]' -c
[[4105,0,0],[4646,0,0],[4666,0,0],[33696,0,0]...]

% curl -s https://raw.githubusercontent.com/jsonata-js/jsonata/master/test/test-suite/datasets/dataset1.json | jsonata '{"Name": FirstName & " " & Surname, "Cities": **.City, "Emails": Email[type="home"].address}'
{
  "Name": "Fred Smith",
  "Cities": [
    "Winchester",
    "London"
  ],
  "Emails": [
    "freddy@my-social.com",
    "frederic.smith@very-serious.com"
  ]
}

% jsonata -i helloworld.json -it
Enter an expression to have it evaluated.
JSONata> (a & b)
hello world
```

## Running Tests

This project uses the repository of the reference implementation as a submodule. This allows referencing the current version of the unit tests. To clone this repository, run:

```
git clone --recurse-submodules https://github.com/rayokota/jsonata-python
```

To build and run the unit tests:

```
python3 -m pip install nox
nox --sessions tests
```

## Notes

JSONata date/time functions that use ISO 8601 formats are only supported with Python 3.11+.

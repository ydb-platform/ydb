![parglare logo](https://raw.githubusercontent.com/igordejanovic/parglare/master/docs/images/parglare-logo.png)

[![build-status](https://github.com/igordejanovic/parglare/actions/workflows/ci-linux-ubuntu.yml/badge.svg)](https://github.com/igordejanovic/parglare/actions)
[![coverage](https://coveralls.io/repos/github/igordejanovic/parglare/badge.svg?branch=master)](https://coveralls.io/github/igordejanovic/parglare?branch=master)
[![docs](https://img.shields.io/badge/docs-latest-green.svg)](http://www.igordejanovic.net/parglare/latest/)
![status](https://img.shields.io/pypi/status/parglare.svg)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![python-versions](https://img.shields.io/pypi/pyversions/parglare.svg)

A pure Python scannerless LR/GLR parser.

For more information see [the docs](http://www.igordejanovic.net/parglare/).

## Quick intro

This is just a small example to get the general idea. This example shows how to
parse and evaluate expressions with 5 operations with different priority and
associativity. Evaluation is done using semantic/reduction actions.

The whole expression evaluator is done in under 30 lines of code!

```python
from parglare import Parser, Grammar

grammar = r"""
E: E '+' E  {left, 1}
 | E '-' E  {left, 1}
 | E '*' E  {left, 2}
 | E '/' E  {left, 2}
 | E '^' E  {right, 3}
 | '(' E ')'
 | number;

terminals
number: /\d+(\.\d+)?/;
"""

actions = {
    "E": [lambda _, n: n[0] + n[2],
          lambda _, n: n[0] - n[2],
          lambda _, n: n[0] * n[2],
          lambda _, n: n[0] / n[2],
          lambda _, n: n[0] ** n[2],
          lambda _, n: n[1],
          lambda _, n: n[0]],
    "number": lambda _, value: float(value),
}

g = Grammar.from_string(grammar)
parser = Parser(g, debug=True, actions=actions)

result = parser.parse("34 + 4.6 / 2 * 4^2^2 + 78")

print("Result = ", result)

# Output
# -- Debugging/tracing output with detailed info about grammar, productions,
# -- terminals and nonterminals, DFA states, parsing progress,
# -- and at the end of the output:
# Result = 700.8
```

## Installation

- Stable version:

  ```
  $ pip install parglare
  ```

- Development:

  Install [just](https://github.com/casey/just).

  ```
  $ git clone git@github.com:igordejanovic/parglare.git
  $ cd parglare
  $ just dev
  ```

## Citing parglare

If you use parglare in your research please cite this paper:

```text
Igor Dejanović, Parglare: A LR/GLR parser for Python,
Science of Computer Programming, issn:0167-6423, p.102734,
DOI:10.1016/j.scico.2021.102734, 2021.
```

```bibtex
@article{dejanovic2021b,
    author = {Igor Dejanović},
    title = {Parglare: A LR/GLR parser for Python},
    doi = {10.1016/j.scico.2021.102734},
    issn = {0167-6423},
    journal = {Science of Computer Programming},
    keywords = {parsing, LR, GLR, Python, visualization},
    pages = {102734},
    url = {https://www.sciencedirect.com/science/article/pii/S0167642321001271},
    year = {2021}
}
```

## License

MIT

## Python versions

Tested with 3.8-3.14

## Credits

Initial layout/content of this package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [audreyr/cookiecutter-pypackage](https://github.com/audreyr/cookiecutter-pypackage) project template.

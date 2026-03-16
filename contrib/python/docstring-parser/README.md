docstring_parser
================

[![Build](https://github.com/rr-/docstring_parser/actions/workflows/build.yml/badge.svg)](https://github.com/rr-/docstring_parser/actions/workflows/build.yml)

Parse Python docstrings. Currently support ReST, Google, Numpydoc-style and
Epydoc docstrings.

Example usage:

```python
>>> from docstring_parser import parse
>>>
>>>
>>> docstring = parse(
...     '''
...     Short description
...
...     Long description spanning multiple lines
...     - First line
...     - Second line
...     - Third line
...
...     :param name: description 1
...     :param int priority: description 2
...     :param str sender: description 3
...     :raises ValueError: if name is invalid
...     ''')
>>>
>>> docstring.long_description
'Long description spanning multiple lines\n- First line\n- Second line\n- Third line'
>>> docstring.params[1].arg_name
'priority'
>>> docstring.raises[0].type_name
'ValueError'
```

Read [API Documentation](https://rr-.github.io/docstring_parser/).

# Installation

Installation using pip

```shell
pip install docstring_parser

# or if you want to install it in a virtual environment

python -m venv venv # create environment
source venv/bin/activate # activate environment
python -m pip install docstring_parser
```

Installation using conda


1. Download and install miniconda or anaconda
2. Install the package from the conda-forge channel via:
  - `conda install -c conda-forge docstring_parser`
  - or create a new conda environment via `conda create -n my-new-environment -c conda-forge docstring_parser`


# Contributing

To set up the project:
```sh
git clone https://github.com/rr-/docstring_parser.git
cd docstring_parser

python -m venv venv  # create environment
source venv/bin/activate  # activate environment

pip install -e ".[dev]"  # install as editable
pre-commit install  # make sure pre-commit is setup
```

To run tests:
```
source venv/bin/activate
pytest
```

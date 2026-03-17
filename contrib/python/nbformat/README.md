# nbformat: Jupyter Notebook Format

![CI Tests](https://github.com/jupyter/nbformat/workflows/Run%20tests/badge.svg)

`nbformat` contains the reference implementation of the [Jupyter Notebook format],
and Python APIs for working with notebooks.

There is also a JSON Schema for nbformat versions >= 3.

## Installation

From the command line:

```{.sourceCode .bash}
pip install nbformat
```

## Using a different json schema validator

We use `fastjsonschema` by default. To use `jsonschema` instead, set the environment variable `NBFORMAT_VALIDATOR` to the value `jsonschema`.

## Python Version Support

This library supported Python 2.7 and Python 3.5+ for `4.x.x` releases. With Python 2's end-of-life nbformat `5.x.x` supported Python 3 only. Support for Python 3.x versions will be dropped when they are officially sunset by the python organization.

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on how to setup a local development environment and make code changes back to nbformat.

## About the Jupyter Development Team

The Jupyter Development Team is the set of all contributors to the Jupyter project.
This includes all of the Jupyter subprojects.

The core team that coordinates development on GitHub can be found here:
https://github.com/jupyter/.

## Our Copyright Policy

Jupyter uses a shared copyright model. Each contributor maintains copyright
over their contributions to Jupyter. But, it is important to note that these
contributions are typically only changes to the repositories. Thus, the Jupyter
source code, in its entirety is not the copyright of any single person or
institution. Instead, it is the collective copyright of the entire Jupyter
Development Team. If individual contributors want to maintain a record of what
changes/contributions they have specific copyright on, they should indicate
their copyright in the commit message of the change, when they commit the
change to one of the Jupyter repositories.

With this in mind, the following banner should be used in any source code file
to indicate the copyright and license terms:

```
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
```

[jupyter notebook format]: https://nbformat.readthedocs.org/en/latest/format_description.html

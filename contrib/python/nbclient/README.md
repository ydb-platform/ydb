[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jupyter/nbclient/main?filepath=binder%2Frun_nbclient.ipynb)
[![Build Status](https://github.com/jupyter/nbclient/workflows/CI/badge.svg)](https://github.com/jupyter/nbclient/actions)
[![Documentation Status](https://readthedocs.org/projects/nbclient/badge/?version=latest)](https://nbclient.readthedocs.io/en/latest/?badge=latest)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# nbclient

**NBClient** lets you **execute** notebooks.

A client library for programmatic notebook execution, **NBClient** is a tool for running Jupyter Notebooks in
different execution contexts, including the command line.

## Interactive Demo

To demo **NBClient** interactively, click this Binder badge to start the demo:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jupyter/nbclient/main?filepath=binder%2Frun_nbclient.ipynb)

## Installation

In a terminal, run:

```
python3 -m pip install nbclient
```

## Documentation

See [ReadTheDocs](https://nbclient.readthedocs.io/en/latest/) for more in-depth details about the project and the
[API Reference](https://nbclient.readthedocs.io/en/latest/reference/index.html).

## Python Version Support

This library currently supports Python 3.6+ versions. As minor Python
versions are officially sunset by the Python org, nbclient will similarly
drop support in the future.

## Origins

This library used to be part of the [nbconvert](https://nbconvert.readthedocs.io/en/latest/) project. NBClient extracted nbconvert's `ExecutePreprocessor`into its own library for easier updating and importing by downstream libraries and applications.

## Relationship to JupyterClient

NBClient and JupyterClient are distinct projects.

`jupyter_client` is a client library for the jupyter protocol. Specifically, `jupyter_client` provides the Python API
for starting, managing and communicating with Jupyter kernels.

While, nbclient allows notebooks to be run in different execution contexts.

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
institution.  Instead, it is the collective copyright of the entire Jupyter
Development Team.  If individual contributors want to maintain a record of what
changes/contributions they have specific copyright on, they should indicate
their copyright in the commit message of the change, when they commit the
change to one of the Jupyter repositories.

With this in mind, the following banner should be used in any source code file
to indicate the copyright and license terms:

```
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
```

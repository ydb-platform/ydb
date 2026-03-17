# Jupyter Server

[![Build Status](https://github.com/jupyter-server/jupyter_server/actions/workflows/python-tests.yml/badge.svg?query=branch%3Amain++)](https://github.com/jupyter-server/jupyter_server/actions/workflows/python-tests.yml/badge.svg?query=branch%3Amain++)
[![Documentation Status](https://readthedocs.org/projects/jupyter-server/badge/?version=latest)](http://jupyter-server.readthedocs.io/en/latest/?badge=latest)

The Jupyter Server provides the backend (i.e. the core services, APIs, and REST endpoints) for Jupyter web applications like Jupyter notebook, JupyterLab, and Voila.

For more information, read our [documentation here](http://jupyter-server.readthedocs.io/en/latest/?badge=latest).

## Installation and Basic usage

To install the latest release locally, make sure you have
[pip installed](https://pip.readthedocs.io/en/stable/installing/) and run:

```
pip install jupyter_server
```

Jupyter Server currently supports Python>=3.6 on Linux, OSX and Windows.

### Versioning and Branches

If Jupyter Server is a dependency of your project/application, it is important that you pin it to a version that works for your application. Currently, Jupyter Server only has minor and patch versions. Different minor versions likely include API-changes while patch versions do not change API.

When a new minor version is released on PyPI, a branch for that version will be created in this repository, and the version of the main branch will be bumped to the next minor version number. That way, the main branch always reflects the latest un-released version.

To see the changes between releases, checkout the [CHANGELOG](https://github.com/jupyter/jupyter_server/blob/main/CHANGELOG.md).

## Usage - Running Jupyter Server

### Running in a local installation

Launch with:

```
jupyter server
```

### Testing

See [CONTRIBUTING](https://github.com/jupyter-server/jupyter_server/blob/main/CONTRIBUTING.rst#running-tests).

## Contributing

If you are interested in contributing to the project, see [`CONTRIBUTING.rst`](CONTRIBUTING.rst).

## Team Meetings and Roadmap

- When: Thursdays [8:00am, Pacific time](https://www.thetimezoneconverter.com/?t=8%3A00%20am&tz=San%20Francisco&)
- Where: [Jovyan Zoom](https://zoom.us/j/95228013874?pwd=Ep7HIk8t9JP6VToxt1Wj4P7K5PshC0.1)
- What:
  - [Meeting notes](https://github.com/jupyter-server/team-compass/issues?q=is%3Aissue%20%20Meeting%20Notes%20)
  - [Agenda](https://hackmd.io/Wmz_wjrLRHuUbgWphjwRWw)

See our tentative [roadmap here](https://github.com/jupyter/jupyter_server/issues/127).

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

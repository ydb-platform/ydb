# python-makefun

*Small library to dynamically create python functions.*

[![Python versions](https://img.shields.io/pypi/pyversions/makefun.svg)](https://pypi.python.org/pypi/makefun/) [![Build Status](https://github.com/smarie/python-makefun/actions/workflows/base.yml/badge.svg)](https://github.com/smarie/python-makefun/actions/workflows/base.yml) [![Tests Status](https://smarie.github.io/python-makefun/reports/junit/junit-badge.svg?dummy=8484744)](https://smarie.github.io/python-makefun/reports/junit/report.html) [![Coverage Status](https://smarie.github.io/python-makefun/reports/coverage/coverage-badge.svg?dummy=8484744)](https://smarie.github.io/python-makefun/reports/coverage/index.html) [![codecov](https://codecov.io/gh/smarie/python-makefun/branch/main/graph/badge.svg)](https://codecov.io/gh/smarie/python-makefun) [![Flake8 Status](https://smarie.github.io/python-makefun/reports/flake8/flake8-badge.svg?dummy=8484744)](https://smarie.github.io/python-makefun/reports/flake8/index.html)

[![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://smarie.github.io/python-makefun/) [![PyPI](https://img.shields.io/pypi/v/makefun.svg)](https://pypi.python.org/pypi/makefun/) [![Downloads](https://pepy.tech/badge/makefun)](https://pepy.tech/project/makefun) [![Downloads per week](https://pepy.tech/badge/makefun/week)](https://pepy.tech/project/makefun) [![GitHub stars](https://img.shields.io/github/stars/smarie/python-makefun.svg)](https://github.com/smarie/python-makefun/stargazers)

**This is the readme for developers.** The documentation for users is available here: [https://smarie.github.io/python-makefun/](https://smarie.github.io/python-makefun/)

## Want to contribute ?

Contributions are welcome ! Simply fork this project on github, commit your contributions, and create pull requests.

Here is a non-exhaustive list of interesting open topics: [https://github.com/smarie/python-makefun/issues](https://github.com/smarie/python-makefun/issues)

## `nox` setup

This project uses `nox` to define all lifecycle tasks. In order to be able to run those tasks, you should create python 3.7 environment and install the requirements:

```bash
>>> conda create -n noxenv python="3.7"
>>> activate noxenv
(noxenv) >>> pip install -r noxfile-requirements.txt
```

You should then be able to list all available tasks using:

```
>>> nox --list
Sessions defined in <path>\noxfile.py:

* tests-2.7 -> Run the test suite, including test reports generation and coverage reports.
* tests-3.5 -> Run the test suite, including test reports generation and coverage reports.
* tests-3.6 -> Run the test suite, including test reports generation and coverage reports.
* tests-3.8 -> Run the test suite, including test reports generation and coverage reports.
* tests-3.7 -> Run the test suite, including test reports generation and coverage reports.
- docs-3.7 -> Generates the doc and serves it on a local http server. Pass '-- build' to build statically instead.
- publish-3.7 -> Deploy the docs+reports on github pages. Note: this rebuilds the docs
- release-3.7 -> Create a release on github corresponding to the latest tag
```

## Running the tests and generating the reports

This project uses `pytest` so running `pytest` at the root folder will execute all tests on current environment. However it is a bit cumbersome to manage all requirements by hand ; it is easier to use `nox` to run `pytest` on all supported python environments with the correct package requirements:

```bash
nox
```

Tests and coverage reports are automatically generated under `./docs/reports` for one of the sessions (`tests-3.7`). 

If you wish to execute tests on a specific environment, use explicit session names, e.g. `nox -s tests-3.6`.


## Editing the documentation

This project uses `mkdocs` to generate its documentation page. Therefore building a local copy of the doc page may be done using `mkdocs build -f docs/mkdocs.yml`. However once again things are easier with `nox`. You can easily build and serve locally a version of the documentation site using:

```bash
>>> nox -s docs
nox > Running session docs-3.7
nox > Creating conda env in .nox\docs-3-7 with python=3.7
nox > [docs] Installing requirements with pip: ['mkdocs-material', 'mkdocs', 'pymdown-extensions', 'pygments']
nox > python -m pip install mkdocs-material mkdocs pymdown-extensions pygments
nox > mkdocs serve -f ./docs/mkdocs.yml
INFO    -  Building documentation...
INFO    -  Cleaning site directory
INFO    -  The following pages exist in the docs directory, but are not included in the "nav" configuration:
  - long_description.md
INFO    -  Documentation built in 1.07 seconds
INFO    -  Serving on http://127.0.0.1:8000
INFO    -  Start watching changes
...
```

While this is running, you can edit the files under `./docs/` and browse the automatically refreshed documentation at the local [http://127.0.0.1:8000](http://127.0.0.1:8000) page.

Once you are done, simply hit `<CTRL+C>` to stop the session.

Publishing the documentation (including tests and coverage reports) is done automatically by [the continuous integration engine](https://github.com/smarie/python-makefun/actions), using the `nox -s publish` session, this is not needed for local development.

## Packaging

This project uses `setuptools_scm` to synchronise the version number. Therefore the following command should be used for development snapshots as well as official releases: `python setup.py sdist bdist_wheel`. However this is not generally needed since [the continuous integration engine](https://github.com/smarie/python-makefun/actions) does it automatically for us on git tags. For reference, this is done in the `nox -s release` session.

### Merging pull requests with edits - memo

Ax explained in github ('get commandline instructions'):

```bash
git checkout -b <git_name>-<feature_branch> main
git pull https://github.com/<git_name>/python-makefun.git <feature_branch> --no-commit --ff-only
```

if the second step does not work, do a normal auto-merge (do not use **rebase**!):

```bash
git pull https://github.com/<git_name>/python-makefun.git <feature_branch> --no-commit
```

Finally review the changes, possibly perform some modifications, and commit.

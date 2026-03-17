<a href="https://explosion.ai"><img src="https://explosion.ai/assets/img/logo.svg" width="125" height="125" align="right" /></a>

# Thinc: A refreshing functional take on deep learning, compatible with your favorite libraries

### From the makers of [spaCy](https://spacy.io) and [Prodigy](https://prodi.gy)

[Thinc](https://thinc.ai) is a **lightweight deep learning library** that offers
an elegant, type-checked, functional-programming API for **composing models**,
with support for layers defined in other frameworks such as **PyTorch,
TensorFlow and MXNet**. You can use Thinc as an interface layer, a standalone
toolkit or a flexible way to develop new models. Previous versions of Thinc have
been running quietly in production in thousands of companies, via both
[spaCy](https://spacy.io) and [Prodigy](https://prodi.gy). We wrote the new
version to let users **compose, configure and deploy custom models** built with
their favorite framework.

[![tests](https://github.com/explosion/thinc/actions/workflows/tests.yml/badge.svg)](https://github.com/explosion/thinc/actions/workflows/tests.yml)
[![Current Release Version](https://img.shields.io/github/v/release/explosion/thinc.svg?include_prereleases&sort=semver&style=flat-square&logo=github)](https://github.com/explosion/thinc/releases)
[![PyPi Version](https://img.shields.io/pypi/v/thinc.svg?include_prereleases&sort=semver&style=flat-square&logo=pypi&logoColor=white)](https://pypi.python.org/pypi/thinc)
[![conda Version](https://img.shields.io/conda/vn/conda-forge/thinc.svg?style=flat-square&logo=conda-forge&logoColor=white)](https://anaconda.org/conda-forge/thinc)
[![Python wheels](https://img.shields.io/badge/wheels-%E2%9C%93-4c1.svg?longCache=true&style=flat-square&logo=python&logoColor=white)](https://github.com/explosion/wheelwright/releases)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/ambv/black)
[![Open demo in Colab][colab]][intro_to_thinc_colab]

## 🔥 Features

- **Type-check** your model definitions with custom types and
  [`mypy`](https://mypy.readthedocs.io/en/latest/) plugin.
- Wrap **PyTorch**, **TensorFlow** and **MXNet** models for use in your network.
- Concise **functional-programming** approach to model definition, using
  composition rather than inheritance.
- Optional custom infix notation via **operator overloading**.
- Integrated **config system** to describe trees of objects and hyperparameters.
- Choice of **extensible backends**.
- **[Read more &rarr;](https://thinc.ai/docs)**

## 🚀 Quickstart

Thinc is compatible with **Python 3.6+** and runs on **Linux**, **macOS** and
**Windows**. The latest releases with binary wheels are available from
[pip](https://pypi.python.org/pypi/thinc). Before you install Thinc and its
dependencies, make sure that your `pip`, `setuptools` and `wheel` are up to
date. For the most recent releases, pip 19.3 or newer is recommended.

```bash
pip install -U pip setuptools wheel
pip install thinc
```

See the [extended installation docs](https://thinc.ai/docs/install#extended) for
details on optional dependencies for different backends and GPU. You might also
want to
[set up static type checking](https://thinc.ai/docs/install#type-checking) to
take advantage of Thinc's type system.

> ⚠️ If you have installed PyTorch and you are using Python 3.7+, uninstall the
> package `dataclasses` with `pip uninstall dataclasses`, since it may have been
> installed by PyTorch and is incompatible with Python 3.7+.

### 📓 Selected examples and notebooks

Also see the [`/examples`](examples) directory and
[usage documentation](https://thinc.ai/docs) for more examples. Most examples
are Jupyter notebooks – to launch them on
[Google Colab](https://colab.research.google.com) (with GPU support!) click on
the button next to the notebook name.

| Notebook                                                                                                              | Description                                                                                                                                                                                       |
| --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`intro_to_thinc`][intro_to_thinc]<br />[![Open in Colab][colab]][intro_to_thinc_colab]                               | Everything you need to know to get started. Composing and training a model on the MNIST data, using config files, registering custom functions and wrapping PyTorch, TensorFlow and MXNet models. |
| [`transformers_tagger_bert`][transformers_tagger_bert]<br />[![Open in Colab][colab]][transformers_tagger_bert_colab] | How to use Thinc, `transformers` and PyTorch to train a part-of-speech tagger. From model definition and config to the training loop.                                                             |
| [`pos_tagger_basic_cnn`][pos_tagger_basic_cnn]<br />[![Open in Colab][colab]][pos_tagger_basic_cnn_colab]             | Implementing and training a basic CNN for part-of-speech tagging model without external dependencies and using different levels of Thinc's config system.                                         |
| [`parallel_training_ray`][parallel_training_ray]<br />[![Open in Colab][colab]][parallel_training_ray_colab]          | How to set up synchronous and asynchronous parameter server training with Thinc and [Ray](https://ray.readthedocs.io/en/latest/).                                                                 |

**[View more &rarr;](examples)**

[colab]:
  https://gistcdn.githack.com/ines/dcf354aa71a7665ae19871d7fd14a4e0/raw/461fc1f61a7bc5860f943cd4b6bcfabb8c8906e7/colab-badge.svg
[intro_to_thinc]: examples/00_intro_to_thinc.ipynb
[intro_to_thinc_colab]:
  https://colab.research.google.com/github/explosion/thinc/blob/master/examples/00_intro_to_thinc.ipynb
[transformers_tagger_bert]: examples/02_transformers_tagger_bert.ipynb
[transformers_tagger_bert_colab]:
  https://colab.research.google.com/github/explosion/thinc/blob/master/examples/02_transformers_tagger_bert.ipynb
[pos_tagger_basic_cnn]: examples/03_pos_tagger_basic_cnn.ipynb
[pos_tagger_basic_cnn_colab]:
  https://colab.research.google.com/github/explosion/thinc/blob/master/examples/03_pos_tagger_basic_cnn.ipynb
[parallel_training_ray]: examples/04_parallel_training_ray.ipynb
[parallel_training_ray_colab]:
  https://colab.research.google.com/github/explosion/thinc/blob/master/examples/04_parallel_training_ray.ipynb

### 📖 Documentation & usage guides

| Documentation                                                                     | Description                                           |
| --------------------------------------------------------------------------------- | ----------------------------------------------------- |
| [Introduction](https://thinc.ai/docs)                                             | Everything you need to know.                          |
| [Concept & Design](https://thinc.ai/docs/concept)                                 | Thinc's conceptual model and how it works.            |
| [Defining and using models](https://thinc.ai/docs/usage-models)                   | How to compose models and update state.               |
| [Configuration system](https://thinc.ai/docs/usage-config)                        | Thinc's config system and function registry.          |
| [Integrating PyTorch, TensorFlow & MXNet](https://thinc.ai/docs/usage-frameworks) | Interoperability with machine learning frameworks     |
| [Layers API](https://thinc.ai/docs/api-layers)                                    | Weights layers, transforms, combinators and wrappers. |
| [Type Checking](https://thinc.ai/docs/usage-type-checking)                        | Type-check your model definitions and more.           |

## 🗺 What's where

| Module                                    | Description                                                                       |
| ----------------------------------------- | --------------------------------------------------------------------------------- |
| [`thinc.api`](thinc/api.py)               | **User-facing API.** All classes and functions should be imported from here.      |
| [`thinc.types`](thinc/types.py)           | Custom [types and dataclasses](https://thinc.ai/docs/api-types).                  |
| [`thinc.model`](thinc/model.py)           | The `Model` class. All Thinc models are an instance (not a subclass) of `Model`.  |
| [`thinc.layers`](thinc/layers)            | The layers. Each layer is implemented in its own module.                          |
| [`thinc.shims`](thinc/shims)              | Interface for external models implemented in PyTorch, TensorFlow etc.             |
| [`thinc.loss`](thinc/loss.py)             | Functions to calculate losses.                                                    |
| [`thinc.optimizers`](thinc/optimizers.py) | Functions to create optimizers. Currently supports "vanilla" SGD, Adam and RAdam. |
| [`thinc.schedules`](thinc/schedules.py)   | Generators for different rates, schedules, decays or series.                      |
| [`thinc.backends`](thinc/backends)        | Backends for `numpy` and `cupy`.                                                  |
| [`thinc.config`](thinc/config.py)         | Config parsing and validation and function registry system.                       |
| [`thinc.util`](thinc/util.py)             | Utilities and helper functions.                                                   |

## 🐍 Development notes

Thinc uses [`black`](https://github.com/psf/black) for auto-formatting,
[`flake8`](http://flake8.pycqa.org/en/latest/) for linting and
[`mypy`](https://mypy.readthedocs.io/en/latest/) for type checking. All code is
written compatible with **Python 3.6+**, with type hints wherever possible. See
the [type reference](https://thinc.ai/docs/api-types) for more details on
Thinc's custom types.

### 👷‍♀️ Building Thinc from source

Building Thinc from source requires the full dependencies listed in
[`requirements.txt`](requirements.txt) to be installed. You'll also need a
compiler to build the C extensions.

```bash
git clone https://github.com/explosion/thinc
cd thinc
python -m venv .env
source .env/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements.txt
pip install --no-build-isolation .
```

Alternatively, install in editable mode:

```bash
pip install -r requirements.txt
pip install --no-build-isolation --editable .
```

Or by setting `PYTHONPATH`:

```bash
export PYTHONPATH=`pwd`
pip install -r requirements.txt
python setup.py build_ext --inplace
```

### 🚦 Running tests

Thinc comes with an [extensive test suite](thinc/tests). The following should
all pass and not report any warnings or errors:

```bash
python -m pytest thinc    # test suite
python -m mypy thinc      # type checks
python -m flake8 thinc    # linting
```

To view test coverage, you can run `python -m pytest thinc --cov=thinc`. We aim
for a 100% test coverage. This doesn't mean that we meticulously write tests for
every single line – we ignore blocks that are not relevant or difficult to test
and make sure that the tests execute all code paths.

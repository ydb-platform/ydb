# nbconvert
### Jupyter Notebook Conversion

[![Google Group](https://img.shields.io/badge/-Google%20Group-lightgrey.svg)](https://groups.google.com/forum/#!forum/jupyter)
[![Build Status](https://travis-ci.org/jupyter/nbconvert.svg?branch=main)](https://travis-ci.org/jupyter/nbconvert)
[![Documentation Status](https://readthedocs.org/projects/nbconvert/badge/?version=latest)](https://nbconvert.readthedocs.io/en/latest/?badge=latest)
[![Documentation Status](https://readthedocs.org/projects/nbconvert/badge/?version=stable)](https://nbconvert.readthedocs.io/en/stable/?badge=stable)
[![codecov.io](https://codecov.io/github/jupyter/nbconvert/coverage.svg?branch=main)](https://codecov.io/github/jupyter/nbconvert?branch=main)
[![CircleCI Docs Status](https://circleci.com/gh/jupyter/nbconvert/tree/main.svg?style=svg)](https://circleci.com/gh/jupyter/nbconvert/tree/main)

The **nbconvert** tool, `jupyter nbconvert`, converts notebooks to various other
formats via [Jinja][] templates. The nbconvert tool allows you to convert an
`.ipynb` notebook file into various static formats including:

* HTML
* LaTeX
* PDF
* Reveal JS
* Markdown (md)
* ReStructured Text (rst)
* executable script

## Usage

From the command line, use nbconvert to convert a Jupyter notebook (*input*) to a
a different format (*output*). The basic command structure is:

    $ jupyter nbconvert --to <output format> <input notebook>

where `<output format>` is the desired output format and `<input notebook>` is the
filename of the Jupyter notebook.

### Example: Convert a notebook to HTML

Convert Jupyter notebook file, `mynotebook.ipynb`, to HTML using:

    $ jupyter nbconvert --to html mynotebook.ipynb

This command creates an HTML output file named `mynotebook.html`.

## Dev Install

Check if pandoc is installed (``pandoc --version``); if needed, install:

```
sudo apt-get install pandoc
```

Or

```
brew install pandoc
```

Install nbconvert for development using:

```
git clone https://github.com/jupyter/nbconvert.git
cd nbconvert
pip install -e .
```

Running the tests after a dev install above:

```
pip install nbconvert[test]
py.test --pyargs nbconvert
```

## Documentation

- [Documentation for Jupyter nbconvert](https://nbconvert.readthedocs.io/en/latest/)
  [[PDF](https://media.readthedocs.org/pdf/nbconvert/latest/nbconvert.pdf)]
- [nbconvert examples on GitHub](https://github.com/jupyter/nbconvert-examples)
- [Documentation for Project Jupyter](https://jupyter.readthedocs.io/en/latest/index.html)
  [[PDF](https://media.readthedocs.org/pdf/jupyter/latest/jupyter.pdf)]

## Technical Support

- [Issues and Bug Reports](https://github.com/jupyter/nbconvert/issues): A place to report
  bugs or regressions found for nbconvert
- [Community Technical Support and Discussion - Discourse](https://discourse.jupyter.org/): A place for
  installation, configuration, and troubleshooting assistannce by the Jupyter community.
  As a non-profit project and maintainers who are primarily volunteers, we encourage you
  to ask questions and share your knowledge on Discourse.

## Jupyter Resources

- [Jupyter mailing list](https://groups.google.com/forum/#!forum/jupyter)
- [Project Jupyter website](https://jupyter.org)

[Jinja]: http://jinja.pocoo.org/

# StanIO

[![codecov](https://codecov.io/gh/stan-dev/stanio/graph/badge.svg?token=P93MLO21FK)](https://codecov.io/gh/stan-dev/stanio) [![Tests](https://github.com/stan-dev/stanio/actions/workflows/test.yaml/badge.svg)](https://github.com/stan-dev/stanio/actions/workflows/test.yaml)

A set of Python functions for data-wrangling in the formats used by
the [Stan](https://mc-stan.org) probabalistic programming language.

It is primarily developed for use in [cmdstanpy](https://github.com/stan-dev/cmdstanpy).

## Features

- Writing Python dictionaries to Stan-compatible JSON (with optional support for using `ujson` for faster serialization)
- Basic reading of StanCSV files into numpy arrays
- Parameter extraction from numpy arrays based on StanCSV headers
  - e.g., if you have a `matrix[2,3] x` in a Stan program, extracting `x` will give you a `(num_draws, 2, 3)` numpy array

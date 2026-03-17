# jupyterlab server

[![Build Status](https://github.com/jupyterlab/jupyterlab_server/workflows/Tests/badge.svg?branch=master)](https://github.com/jupyterlab/jupyterlab_server/actions?query=branch%3Amaster+workflow%3A%22Tests%22)
[![Documentation Status](https://readthedocs.org/projects/jupyterlab-server/badge/?version=stable)](http://jupyterlab-server.readthedocs.io/en/stable/)

## Motivation

JupyterLab Server sits between JupyterLab and Jupyter Server, and provides a
set of REST API handlers and utilities that are used by JupyterLab. It is a separate project in order to
accommodate creating JupyterLab-like applications from a more limited scope.

## Install

`pip install jupyterlab_server`

To include optional `openapi` dependencies, use:

`pip install jupyterlab_server[openapi]`

To include optional `pytest_plugin` dependencies, use:

`pip install jupyterlab_server[test]`

## Usage

See the full documentation for [API docs](https://jupyterlab-server.readthedocs.io/en/stable/api/index.html) and [REST endpoint descriptions](https://jupyterlab-server.readthedocs.io/en/stable/api/rest.html).

## Extending the Application

Subclass the `LabServerApp` and provide additional traits and handlers as appropriate for your application.

## Contribution

Please see `CONTRIBUTING.md` for details.

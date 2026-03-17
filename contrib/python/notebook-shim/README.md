# Notebook Shim

This project provides a way for JupyterLab and other frontends to switch to [Jupyter Server](https://github.com/jupyter/jupyter_server/) for their Python Web application backend.

## Basic Usage

Install from PyPI:

```
pip install notebook_shim
```

This will automatically enable the extension in Jupyter Server.

## Usage

This project also includes an API for shimming traits that moved from `NotebookApp` in to `ServerApp` in Jupyter Server. This can be used by applications that subclassed `NotebookApp` to leverage the Python server backend of Jupyter Notebooks. Such extensions should *now* switch to `ExtensionApp` API in Jupyter Server and add `NotebookConfigShimMixin` in their inheritance list to properly handle moved traits.

For example, an application class that previously looked like:

```python
from notebook.notebookapp import NotebookApp

class MyApplication(NotebookApp):
```

should switch to look something like:

```python
from jupyter_server.extension.application import ExtensionApp
from notebook_shim.shim import NotebookConfigShimMixin

class MyApplication(NotebookConfigShimMixin, ExtensionApp):
```
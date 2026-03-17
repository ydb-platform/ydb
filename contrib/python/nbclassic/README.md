# The Classic Jupyter Notebook as a Jupyter Server Extension

![Testing nbclassic](https://github.com/jupyterlab/nbclassic/workflows/Testing%20nbclassic/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/nbclassic/badge/?version=latest)](https://nbclassic.readthedocs.io/en/latest/?badge=latest)

*Read the full [NbClassic User Manual here]!*

The Jupyter Notebook is [evolving to bring you big new features], but it
will also break backwards compatibility with many classic Jupyter Notebook
extensions and customizations.

NbClassic provides a backwards compatible Jupyter Notebook interface that
you can [install side-by-side] with the latest versions: That way, you can
fearlessly upgrade without worrying about your classic extensions and
customizations breaking.

## How does it work?

Because NbClassic provides the classic interface on top of the new [Jupyter
Server] backend, it can coexist with other frontends like JupyterLab and
Notebook 7 in the same installation. NbClassic preserves the custom classic
notebook experience under a new set of URL endpoints, under the namespace
`/nbclassic/`.

## Basic Usage

Install from PyPI:

```
> pip install nbclassic
```

This will automatically enable the NbClassic Jupyter Server extension in Jupyter Server.

Launch directly:

```
> jupyter nbclassic
```

Alternatively, you can run Jupyter Server:

```
> jupyter server
```

[Jupyter Server]: https://github.com/jupyter/jupyter_server/
[evolving to bring you big new features]: https://jupyter-notebook.readthedocs.io/en/latest/migrate_to_notebook7.html
[NbClassic User Manual here]: https://nbclassic.readthedocs.io/en/latest/
[install side-by-side]: https://jupyter-notebook.readthedocs.io/en/latest/migrating/multiple-interfaces.html

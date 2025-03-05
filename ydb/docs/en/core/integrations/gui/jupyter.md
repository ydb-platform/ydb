# Work with {{ ydb-short-name }} from Jupyter Notebook

[Jupyter Notebook](https://jupyter.org) is an open-source tool for creating shareable documents that combine code, plain language descriptions, data, rich visualizations, and interactive controls.

The [ydb-sqlalchemy dialect](https://github.com/ydb-platform/ydb-sqlalchemy/releases) enables working with {{ ydb-short-name }} from tools such as:

* [Pandas](https://pandas.pydata.org/)
* [JupySQL](https://jupysql.ploomber.io/)

## Example

A detailed usage example is available as a [notebook](https://github.com/ydb-platform/ydb-sqlalchemy/blob/main/examples/jupyter_notebook/YDB%20SQLAlchemy%20%2B%20Jupyter%20Notebook%20Example.ipynb).

Prerequisites:

1. Python 3.8+
2. [Jupyter Notebook](https://jupyter.org/install#jupyter-notebook)
3. Existing {{ ydb-short-name }} cluster, a single-node one from [quickstart](../../quickstart.md) will suffice

To run the example, download the notebook file <!-- markdownlint-disable no-bare-urls -->{% file src="https://raw.githubusercontent.com/ydb-platform/ydb-sqlalchemy/refs/heads/main/examples/jupyter_notebook/YDB%20SQLAlchemy%20%2B%20Jupyter%20Notebook%20Example.ipynb" name="YDB SQLAlchemy - Jupyter Notebook Example.ipynb" %}, open it in Jupyter, and follow each cell sequentially, executing code as necessary.

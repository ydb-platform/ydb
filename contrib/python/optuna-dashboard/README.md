# optuna-dashboard

![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/optuna-dashboard)](https://pypistats.org/packages/optuna-dashboard)
[![Read the Docs](https://readthedocs.org/projects/optuna-dashboard/badge/?version=latest)](https://optuna-dashboard.readthedocs.io/en/latest/?badge=latest)

:link: [**Website**](https://optuna.org/)
| :page_with_curl: [**Docs**](https://optuna-dashboard.readthedocs.io/en/stable/)
| :gear: [**Install Guide**](https://optuna-dashboard.readthedocs.io/en/stable/getting-started.html#installation)
| :pencil: [**Tutorial**](https://optuna-dashboard.readthedocs.io/en/stable/tutorials/index.html)
| :bulb: [**Examples**](https://github.com/optuna/optuna-examples/tree/main/dashboard)

Real-time dashboard for [Optuna](https://github.com/optuna/optuna).
Code files were originally taken from [Goptuna](https://github.com/c-bata/goptuna).

## Installation

You can install optuna-dashboard via [PyPI](https://pypi.org/project/optuna-dashboard/) or [Anaconda Cloud](https://anaconda.org/conda-forge/optuna-dashboard).

```
$ pip install optuna-dashboard
```

## Getting Started

First, please specify the storage URL to persistent your study using the [RDB backend](https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/001_rdb.html).

```python
import optuna

def objective(trial):
    x = trial.suggest_float("x", -100, 100)
    y = trial.suggest_categorical("y", [-1, 0, 1])
    return x**2 + y

if __name__ == "__main__":
    study = optuna.create_study(
        storage="sqlite:///db.sqlite3",  # Specify the storage URL here.
        study_name="quadratic-simple"
    )
    study.optimize(objective, n_trials=100)
    print(f"Best value: {study.best_value} (params: {study.best_params})")
```

After running the above script, please execute the `optuna-dashboard` command with Optuna storage URL.

```
$ optuna-dashboard sqlite:///db.sqlite3
Listening on http://localhost:8080/
Hit Ctrl-C to quit.
```

<img src="./docs/_static/optuna-dashboard.gif" style="width:600px;" alt="VSCode Extension">

Please check out [our documentation](https://optuna-dashboard.readthedocs.io/en/latest/getting-started.html) for more details.

## Using an official Docker image

You can also use [an official Docker image](https://github.com/optuna/optuna-dashboard/pkgs/container/optuna-dashboard) instead of setting up your Python environment.
The Docker image only supports SQLite3, MySQL(PyMySQL), and PostgreSQL(Psycopg2).

```
$ docker run -it --rm -p 127.0.0.1:8080:8080 -v `pwd`:/app -w /app \
> ghcr.io/optuna/optuna-dashboard sqlite:///db.sqlite3
```

<details>
<summary>MySQL (PyMySQL)</summary>

```
$ docker run -it --rm -p 127.0.0.1:8080:8080 ghcr.io/optuna/optuna-dashboard mysql+pymysql://username:password@hostname:3306/dbname
```

</details>

<details>
<summary>PostgreSQL (Psycopg2)</summary>

```
$ docker run -it --rm -p 127.0.0.1:8080:8080 ghcr.io/optuna/optuna-dashboard postgresql+psycopg2://username:password@hostname:5432/dbname
```

</details>

## Jupyter Lab Extension (Experimental)

You can install the Jupyter Lab extension via [PyPI](https://pypi.org/project/jupyterlab-optuna/).

```
$ pip install jupyterlab jupyterlab-optuna
```

<img src="./docs/_static/jupyterlab-extension.png" style="width:600px;" alt="Jupyter Lab Extension">

To use, click the tile to launch the extension, and enter your Optuna’s storage URL (e.g. `sqlite:///db.sqlite3`) in the dialog.

## Browser-only version (Experimental)

<img src="./docs/_static/browser-app.gif" style="width:600px;" alt="Browser-only version">

We’ve developed the version that operates solely within your web browser, which internally uses SQLite3 Wasm and Rust.
There’s no need to install Python or any other dependencies.
Simply open the following URL in your browser, drag and drop your SQLite3 file onto the page, and you’re ready to view your Optuna studies!

https://optuna.github.io/optuna-dashboard/

*Please note that only a subset of features is available. However, you can still check the optimization history, hyperparameter importances, and etc. in graphs and tables.*

## VS Code and code-server Extension (Experimental)

You can install the VS Code extension via [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=Optuna.optuna-dashboard#overview),
or install the code-server extension via [Open VSX](https://open-vsx.org/extension/Optuna/optuna-dashboard).

<img src="./docs/_static/vscode-extension.png" style="width:600px;" alt="VS Code Extension">

Please right-click the SQLite3 files (`*.db` or `*.sqlite3`) in the VS Code file explorer and select the "Open in Optuna Dashboard" command from the dropdown menu.
This extension leverages the browser-only version of Optuna Dashboard, so the same limitations apply.

## Submitting patches

If you want to contribute, please check [Developers Guide](./CONTRIBUTING.md).

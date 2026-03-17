<p align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/ewels/rich-click/main/docs/images/rich-click-logo-darkmode.png">
        <img alt="rich-click logo" src="https://raw.githubusercontent.com/ewels/rich-click/main/docs/images/rich-click-logo.png">
    </picture>
</p>
<p align="center">
    <em>Richly rendered command line interfaces in click.</em>
</p>
<p align="center">
    <img src="https://img.shields.io/pypi/v/rich-click?logo=pypi" alt="PyPI"/>
    <img src="https://github.com/ewels/rich-click/workflows/Test%20Coverage/badge.svg" alt="Test Coverage badge">
    <img src="https://github.com/ewels/rich-click/workflows/Lint%20code/badge.svg" alt="Lint code badge">
</p>

---

<p align="center">
    <a href="https://ewels.github.io/rich-click">Documentation</a>&nbsp&nbsp·&nbsp&nbsp<a href="https://github.com/ewels/rich-click">Source Code</a>&nbsp&nbsp·&nbsp&nbsp<a href="https://github.com/ewels/rich-click">Changelog</a>
</p>

---

<!--include-start-->
**rich-click** is a wrapper around [Click](https://click.palletsprojects.com/) that renders help output nicely using [Rich](https://github.com/Textualize/rich).

- Click is a _"Python package for creating beautiful command line interfaces"_.
- Rich is a _"Python library for rich text and beautiful formatting in the terminal"_.

The intention of `rich-click` is to provide attractive help output from
Click, formatted with Rich, with minimal customization required.

## Features

- 🌈 Rich command-line formatting of click help and error messages
- 😌 Same API as Click: usage is simply `import rich_click as click`
- 🎨 Over 100 themes that can be set by developers and end-users (`export RICH_CLICK_THEME=...`)
- 💻 CLI tool to run on _other people's_ Click and Typer CLIs (prefix the command with `rich-click`)
- 📦 Export help text as HTML or SVG
- 🎁 Group commands and options into named panels
- ❌ Well formatted error messages
- 💫 Extensive customization
- 🤖 IDE autocomplete of Click decorators for smooth developer experience

## Installation

```shell
pip install rich-click
```

## Examples

### Simple Example

To use rich-click in your code, replace `import click` with `import rich_click as click` in your existing click CLI:

```python
import rich_click as click

@click.command()
@click.option("--count", default=1, help="Number of greetings.")
@click.option("--name", prompt="Your name", help="The person to greet.")
def hello(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for _ in range(count):
        click.echo(f"Hello, {name}!")

if __name__ == '__main__':
    hello()
```

![`python examples/11_hello.py --help`](docs/images/hello.svg)

_Screenshot from [`examples/11_hello.py`](https://github.com/ewels/rich-click/blob/main/examples/11_hello.py)_

### More complex example

**rich-click** has a ton of customization options that let you compose help text however you'd like.

Below is a more complex example of what **rich-click** is capable of, utilizing **themes** and **panels**:

![`python examples/03_groups_sorting.py --help`](docs/images/command_groups.svg)

_Screenshot from [`examples/03_groups_sorting.py`](https://github.com/ewels/rich-click/blob/main/examples/03_groups_sorting.py)_

## Usage

This is a quick overview of how to use **rich-click**. [Read the docs](https://ewels.github.io/rich-click) for more information.

There are a couple of ways to begin using `rich-click`:

### Import `rich_click` as `click`

Switch out your normal `click` import with `rich_click`, using the same namespace:

```python
import rich_click as click
```

That's it! ✨ Then continue to use Click as you would normally.

> See [`examples/01_simple.py`](https://github.com/ewels/rich-click/blob/main/examples/01_simple.py) for an example.

### Declarative

If you prefer, you can use `RichGroup` or `RichCommand` with the `cls` argument in your click usage instead.
This means that you can continue to use the unmodified `click` package in parallel.

```python
import click
from rich_click import RichCommand

@click.command(cls=RichCommand)
def main():
    """My amazing tool does all the things."""
```

> See [`examples/02_declarative.py`](https://github.com/ewels/rich-click/blob/main/examples/02_declarative.py) for an example.

### `rich-click` CLI tool

**rich-click** comes with a CLI tool that allows you to format the Click help output from _any_ package that uses Click.

To use, prefix `rich-click` to your normal command.
For example, to get richified Click help text from a package called `awesometool`, you could run:

```console
$ rich-click awesometool --help

Usage: awesometool [OPTIONS]
..more richified output below..
```

## License

This project is licensed under the MIT license.

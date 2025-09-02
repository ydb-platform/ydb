# markdown-it-py

[![Github-CI][github-ci]][github-link]
[![Coverage Status][codecov-badge]][codecov-link]
[![PyPI][pypi-badge]][pypi-link]
[![Conda][conda-badge]][conda-link]
[![PyPI - Downloads][install-badge]][install-link]

<p align="center">
  <img alt="markdown-it-py icon" src="https://raw.githubusercontent.com/executablebooks/markdown-it-py/master/docs/_static/markdown-it-py.svg">
</p>

> Markdown parser done right.

- Follows the __[CommonMark spec](http://spec.commonmark.org/)__ for baseline parsing
- Configurable syntax: you can add new rules and even replace existing ones.
- Pluggable: Adds syntax extensions to extend the parser (see the [plugin list][md-plugins]).
- High speed (see our [benchmarking tests][md-performance])
- Easy to configure for [security][md-security]
- Member of [Google's Assured Open Source Software](https://cloud.google.com/assured-open-source-software/docs/supported-packages)

This is a Python port of [markdown-it], and some of its associated plugins.
For more details see: <https://markdown-it-py.readthedocs.io>.

For details on [markdown-it] itself, see:

- The __[Live demo](https://markdown-it.github.io)__
- [The markdown-it README][markdown-it-readme]

**See also:** [markdown-it-pyrs](https://github.com/chrisjsewell/markdown-it-pyrs) for an experimental Rust binding,
for even more speed!

## Installation

### PIP

```bash
pip install markdown-it-py[plugins]
```

or with extras

```bash
pip install markdown-it-py[linkify,plugins]
```

### Conda

```bash
conda install -c conda-forge markdown-it-py
```

or with extras

```bash
conda install -c conda-forge markdown-it-py linkify-it-py mdit-py-plugins
```

## Usage

### Python API Usage

Render markdown to HTML with markdown-it-py and a custom configuration
with and without plugins and features:

```python
from markdown_it import MarkdownIt
from mdit_py_plugins.front_matter import front_matter_plugin
from mdit_py_plugins.footnote import footnote_plugin

md = (
    MarkdownIt('commonmark', {'breaks':True,'html':True})
    .use(front_matter_plugin)
    .use(footnote_plugin)
    .enable('table')
)
text = ("""
---
a: 1
---

a | b
- | -
1 | 2

A footnote [^1]

[^1]: some details
""")
tokens = md.parse(text)
html_text = md.render(text)

## To export the html to a file, uncomment the lines below:
# from pathlib import Path
# Path("output.html").write_text(html_text)
```

### Command-line Usage

Render markdown to HTML with markdown-it-py from the
command-line:

```console
usage: markdown-it [-h] [-v] [filenames [filenames ...]]

Parse one or more markdown files, convert each to HTML, and print to stdout

positional arguments:
  filenames      specify an optional list of files to convert

optional arguments:
  -h, --help     show this help message and exit
  -v, --version  show program's version number and exit

Interactive:

  $ markdown-it
  markdown-it-py [version 0.0.0] (interactive)
  Type Ctrl-D to complete input, or Ctrl-C to exit.
  >>> # Example
  ... > markdown *input*
  ...
  <h1>Example</h1>
  <blockquote>
  <p>markdown <em>input</em></p>
  </blockquote>

Batch:

  $ markdown-it README.md README.footer.md > index.html

```

## References / Thanks

Big thanks to the authors of [markdown-it]:

- Alex Kocharin [github/rlidwka](https://github.com/rlidwka)
- Vitaly Puzrin [github/puzrin](https://github.com/puzrin)

Also [John MacFarlane](https://github.com/jgm) for his work on the CommonMark spec and reference implementations.

[github-ci]: https://github.com/executablebooks/markdown-it-py/actions/workflows/tests.yml/badge.svg?branch=master
[github-link]: https://github.com/executablebooks/markdown-it-py
[pypi-badge]: https://img.shields.io/pypi/v/markdown-it-py.svg
[pypi-link]: https://pypi.org/project/markdown-it-py
[conda-badge]: https://anaconda.org/conda-forge/markdown-it-py/badges/version.svg
[conda-link]: https://anaconda.org/conda-forge/markdown-it-py
[codecov-badge]: https://codecov.io/gh/executablebooks/markdown-it-py/branch/master/graph/badge.svg
[codecov-link]: https://codecov.io/gh/executablebooks/markdown-it-py
[install-badge]: https://img.shields.io/pypi/dw/markdown-it-py?label=pypi%20installs
[install-link]: https://pypistats.org/packages/markdown-it-py

[CommonMark spec]: http://spec.commonmark.org/
[markdown-it]: https://github.com/markdown-it/markdown-it
[markdown-it-readme]: https://github.com/markdown-it/markdown-it/blob/master/README.md
[md-security]: https://markdown-it-py.readthedocs.io/en/latest/security.html
[md-performance]: https://markdown-it-py.readthedocs.io/en/latest/performance.html
[md-plugins]: https://markdown-it-py.readthedocs.io/en/latest/plugins.html

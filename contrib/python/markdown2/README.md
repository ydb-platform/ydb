Markdown is a light text markup format and a processor to convert that to HTML.
The originator describes it as follows:

> Markdown is a text-to-HTML conversion tool for web writers.
> Markdown allows you to write using an easy-to-read,
> easy-to-write plain text format, then convert it to
> structurally valid XHTML (or HTML).
>
> -- <http://daringfireball.net/projects/markdown/>

This (markdown2) is a fast and complete Python implementation of Markdown. It
was written to closely match the behaviour of the original Perl-implemented
Markdown.pl. Markdown2 also comes with a number of extensions (called
"extras") for things like syntax coloring, tables, header-ids. See the
"Extra Syntax" section below. "markdown2" supports all Python versions
3.5+ (and pypy and jython, though I don't frequently test those).

There is another [Python
markdown.py](https://python-markdown.github.io/). However, at
least at the time this project was started, markdown2.py was faster (see the
[Performance
Notes](https://github.com/trentm/python-markdown2/wiki/Performance-Notes)) and,
to my knowledge, more correct (see [Testing
Notes](https://github.com/trentm/python-markdown2/wiki/Testing-Notes)).
That was a while ago though, so you shouldn't discount Python-markdown from
your consideration.

Follow <a href="https://twitter.com/intent/user?screen_name=trentmick" target="_blank">@trentmick</a>
for updates to python-markdown2.

# Install

To install it in your Python installation run *one* of the following:

    pip install markdown2
    pip install markdown2[all]  # to install all optional dependencies (eg: Pygments for code syntax highlighting)
    pypm install markdown2      # if you use ActivePython (activestate.com/activepython)
    easy_install markdown2      # if this is the best you have
    python setup.py install

However, everything you need to run this is in "lib/markdown2.py". If it is
easier for you, you can just copy that file to somewhere on your PythonPath
(to use as a module) or executable path (to use as a script).


# Quick Usage

As a module:
```python
>>> import markdown2
>>> markdown2.markdown("*boo!*")  # or use `html = markdown_path(PATH)`
'<p><em>boo!</em></p>\n'

>>> from markdown2 import Markdown
>>> markdowner = Markdown()
>>> markdowner.convert("*boo!*")
'<p><em>boo!</em></p>\n'
>>> markdowner.convert("**boom!**")
'<p><strong>boom!</strong></p>\n'
```
As a script (CLI):
```shell
$ python markdown2.py foo.md > foo.html
```
or 
```shell
$ python -m markdown2 foo.md > foo.html
```

I think pip-based installation will enable this as well:
```shell
$ markdown2 foo.md > foo.html
```
See the [project wiki](https://github.com/trentm/python-markdown2/wiki),
[lib/markdown2.py](https://github.com/trentm/python-markdown2/blob/master/lib/markdown2.py)
docstrings and/or `python markdown2.py --help` for more details.


# Extra Syntax (aka extensions)

Many Markdown processors include support for additional optional syntax
(often called "extensions") and markdown2 is no exception. With markdown2 these
are called "extras".  Using the "footnotes" extra as an example, here is how
you use an extra ... as a module:
```shell
$ python markdown2.py --extras footnotes foo.md > foo.html
```
as a script:
```shell
>>> import markdown2
>>> markdown2.markdown("*boo!*", extras=["footnotes"])
'<p><em>boo!</em></p>\n'
```
There are a number of currently implemented extras for tables, footnotes,
syntax coloring of `<pre>`-blocks, auto-linking patterns, table of contents,
Smarty Pants (for fancy quotes, dashes, etc.) and more. See the [Extras
wiki page](https://github.com/trentm/python-markdown2/wiki/Extras) for full
details.


# Project

The python-markdown2 project lives at
<https://github.com/trentm/python-markdown2/>.  (Note: On Mar 6, 2011 this
project was moved from [Google Code](http://code.google.com/p/python-markdown2)
to here on Github.) See also, [markdown2 on the Python Package Index
(PyPI)](http://pypi.python.org/pypi/markdown2).

The change log: <https://github.com/trentm/python-markdown2/blob/master/CHANGES.md>

To report a bug: <https://github.com/trentm/python-markdown2/issues>

# Contributing

We welcome pull requests from the community. Please take a look at the [TODO](https://github.com/trentm/python-markdown2/blob/master/TODO.txt) for opportunities to help this project. For those wishing to submit a pull request to `python-markdown2` please ensure it fulfills the following requirements:

* It must pass PEP8.
* It must include relevant test coverage.
* Bug fixes must include a regression test that exercises the bug.
* The entire test suite must pass.
* The README and/or docs are updated accordingly.


# Test Suite

This markdown implementation passes a fairly extensive test suite. To run it:
```shell
make test
```
The crux of the test suite is a number of "cases" directories -- each with a
set of matching .text (input) and .html (expected output) files. These are:

    tm-cases/                   Tests authored for python-markdown2 (tm=="Trent Mick")
    markdowntest-cases/         Tests from the 3rd-party MarkdownTest package
    php-markdown-cases/         Tests from the 3rd-party MDTest package
    php-markdown-extra-cases/   Tests also from MDTest package

See the [Testing Notes wiki
page](https://github.com/trentm/python-markdown2/wiki/Testing-Notes) for full
details.

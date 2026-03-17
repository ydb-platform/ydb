# pyrepl

This is `pyrepl`, a readline-a-like in pure Python.

[![tests](https://github.com/bretello/pyrepl/actions/workflows/tests.yml/badge.svg)](https://github.com/bretello/pyrepl/actions/workflows/tests.yml)
[![lint](https://github.com/bretello/pyrepl/actions/workflows/lint.yml/badge.svg)](https://github.com/bretello/pyrepl/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/bretello/pyrepl/graph/badge.svg?token=93KG729XHR)](https://codecov.io/gh/bretello/pyrepl)

It requires python 3.8 (or newer) and features:

- sane multi-line editing
- history, with incremental search
- completion, including displaying of available options
- a fairly large subset of the readline emacs-mode keybindings
  (adding more is mostly just a matter of typing)
- a liberal, Python-style, license
- a new python top-level
- no global variables, so you can run two or more independent readers
  without having their histories interfering.
- no hogging of control -- it should be easy to integrate pyrepl into
  YOUR application's event loop.
- generally speaking, a much more interactive experience than readline
  (it's a bit like a cross between readline and emacs's mini-buffer)

There are probably still a few little bugs & misfeatures, but _I_ like
it, and use it as my python top-level most of the time.

To get a feel for it, just execute:

```bash
python pyrepl/pythoni.py
```

(One point that may confuse: because the arrow keys are used to move
up and down in the command currently being edited, you need to use ^P
and ^N to move through the history)

## Installation

If you like what you see, you can install it with the familiar

```console
$ pip install pyrepl
```

which will also install `pythoni` script.

## Changelog

See [CHANGELOG](/CHANGELOG)

# fancycompleter: colorful Python TAB completion

[![Tests](https://github.com/bretello/fancycompleter/actions/workflows/tests.yaml/badge.svg)](https://github.com/bretello/fancycompleter/actions/workflows/tests.yaml)
[![codecov](https://codecov.io/gh/bretello/fancycompleter/graph/badge.svg?token=M70VF5GAP8)](https://codecov.io/gh/bretello/fancycompleter)
[![PyPI version](https://img.shields.io/pypi/v/fancycompleter.svg)](https://pypi.org/project/fancycompleter/)

## What is is?

`fancycompleter` is a module to improve your experience in Python by
adding TAB completion to the interactive prompt. It is an extension of
the stdlib's [rlcompleter](http://docs.python.org/library/rlcompleter.html) module.

Its best feature is that the completions are displayed in different
colors, depending on their type:

![image](https://raw.githubusercontent.com/bretello/fancycompleter/refs/heads/master/screenshot.png)

In the image above, strings are shown in green, functions in blue,
integers and boolean in yellows, `None` in gray, types and classes in
fuchsia. Everything else is plain white.

## Other features

- To save space on screen, `fancycompleter` only shows the characters
  "after the dot". By contrast, in the example above `rlcompleter`
  shows everything prepended by `"sys."`.
- If we press `<TAB>` at the beginning of the line, a real tab
  character is inserted, instead of trying to complete. This is useful
  when typing function bodies or multi-line statements at the prompt.
- Unlike `rlcompleter`, `fancycompleter` **does** complete expressions
  containing dictionary or list indexing. For example,
  `mydict['foo'].<TAB>` works (assuming that `mydict` is a dictionary
  and that it contains the key `'foo'`, of course :-)).
- Starting from Python 2.6, if the completed name is a callable,
  `rlcompleter` automatically adds an open parenthesis `(`. This is
  annoying in case we do not want to really call it, so
  `fancycompleter` disable this behaviour.

## Installation

First, install the module with `pip`:

```bash
pip install fancycompleter
```

Then, at the Python interactive prompt:

```console
>>> import fancycompleter
>>> fancycompleter.interact(persist_history=True)
>>>
```

If you want to enable `fancycompleter` automatically at startup, you can
add those two lines at the end of your
[PYTHONSTARTUP](http://docs.python.org/using/cmdline.html#envvar-PYTHONSTARTUP)
script.

If you do **not** have a `PYTHONSTARTUP` script, the
following command will create one for you in `~/python_startup.py`:

```bash
python -m fancycompleter install
```

On Windows, `install` automatically sets the `PYTHONSTARTUP` environment
variable. On other systems, you need to add the proper command in
`~/.bashrc` or equivalent.

**Note**: depending on your particular system, `interact` might need to
play dirty tricks in order to display colors, although everything should
"just work". In particular, the call to `interact` should be the last
line in the startup file, else the next lines might not be executed. See
section [What is really going on?](#what-is-really-going-on) for
details.

## How do I get colors?

If you are using **PyPy** or **CPython 3.13+**, you can stop reading now,
as `fancycompleter` will work out of the box.

If you are using **CPython on Linux/OSX** (<3.13), `fancycompleter` installs
[`pyrepl`](https://github.com/bretello/pyrepl) as a dependency, and you
should also get colors out of the box. If for some reason you don't want
to use `pyrepl`, you should keep on reading.

By default, in CPython line input and TAB completion are handled by [GNU
readline](https://tiswww.case.edu/php/chet/readline/rltop.html) (at least
on Linux). However, `readline` explicitly strips escape sequences from
the completions, so completions with colors are not displayed correctly.

There are two ways to solve it:

> - (suggested) don't use `readline` at all and rely on
>   [pyrepl](https://github.com/bretello/pyrepl)
> - use a patched version of `readline` to allow colors

By default, `fancycompleter` tries to use `pyrepl` if it finds it. To
get colors you need a recent version, >= 0.8.2.

Starting from version 0.6.1, `fancycompleter` works also on **Windows**,
relying on [pyreadline](https://pypi.python.org/pypi/pyreadline). At the
moment of writing, the latest version of `pyreadline` is 2.1, which does
**not** support colored completions; here is the [pull
request](https://github.com/pyreadline/pyreadline/pull/48) which adds
support for them. To enable colors, you can install `pyreadline` from
[this fork](https://github.com/antocuni/pyreadline) using the following
command:

```bash
pip install --upgrade git+https://github.com/antocuni/pyreadline
```

## Customization

To customize the configuration of `fancycompleter`, you need to put a file
named `.fancycompleterrc.py` in your home directory. The file must
contain a class named `Config` inheriting from `DefaultConfig` and
overriding the desired values.

## What is really going on?

The default and preferred way to get colors is to use `pyrepl`. However,
there is no way to tell CPython to use `pyrepl` instead of the built-in
readline at the interactive prompt: this means that even if we install
our completer inside pyrepl's readline library, the interactive prompt
won't see it.

The issue is simply solved by avoiding to use the built-in prompt:
instead, we use a pure Python replacement based on
[code.InteractiveConsole](https://docs.python.org/3/library/code.html#code.InteractiveConsole).
This brings us also some niceties, such as the ability to do multi-line
editing of the history.

The console is automatically run by `fancycompleter.interact()`,
followed by `sys.exit()`: this way, if we execute it from the script in
`PYTHONSTARTUP`, the interpreter exits as soon as we finish the use the
prompt (e.g. by pressing CTRL-D, or by calling `quit()`). This way, we
avoid to enter the built-in prompt and we get a behaviour which closely
resembles the default one. This is why in this configuration lines after
`fancycompleter.interact()` might not be run.

Note that if we are using `readline` instead of `pyrepl`, the trick is
not needed and thus `interact()` will simply returns, letting the
built-in prompt to show up. The same is true if we are running PyPy, as
its built-in prompt is based on pyrepl anyway.

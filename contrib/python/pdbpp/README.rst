pdb++, a drop-in replacement for pdb
====================================

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
   :target: https://github.com/astral-sh/ruff
   :alt: Ruff

.. image:: https://github.com/bretello/pdbpp/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/bretello/pdbpp/actions/workflows/ci.yml
   :alt: Tests

.. image:: https://codecov.io/gh/pdbpp/pdbpp/graph/badge.svg?token=IOKP5121OU
   :target: https://codecov.io/gh/pdbpp/pdbpp
   :alt: Codecov

.. image:: https://img.shields.io/pypi/v/pdbpp.svg
   :target: https://pypi.org/project/pdbpp/
   :alt: PyPI version


What is it?
-----------

This module is an extension of the pdb_ module of the standard library.  It is
meant to be fully compatible with its predecessor, yet it introduces a number
of new features to make your debugging experience as nice as possible.

.. image:: https://user-images.githubusercontent.com/412005/64484794-2f373380-d20f-11e9-9f04-e1dabf113c6f.png

``pdb++`` features include:

  - colorful TAB completion of Python expressions (through fancycompleter_)

  - optional syntax highlighting of code listings (through Pygments_)

  - `sticky mode`_

  - several new commands to be used from the interactive ``(Pdb++)`` prompt

  - `smart command parsing`_ (hint: have you ever typed ``r`` or ``c`` at the
    prompt to print the value of some variable?)

  - additional convenience functions in the ``pdb`` module, to be used from
    your program

``pdb++`` is meant to be a drop-in replacement for ``pdb``. If you find some
unexpected behavior, please report it as a bug.

.. _pdb: http://docs.python.org/library/pdb.html
.. _fancycompleter: https://github.com/bretello/fancycompleter
.. _Pygments: http://pygments.org/

Installation
------------

    $ pip install pdbpp

Usage
-----

Note that the module is called ``pdb.py`` so that ``pdb++`` will automatically
be used in all places that do ``import pdb`` (e.g. ``pytest --pdb`` will
give you a ``pdb++`` prompt).

The old ``pdb`` module is still available by doing e.g. ``import pdb;
pdb.pdb.set_trace()``.

New interactive commands
------------------------

The following are new commands that you can use from the interactive
``(Pdb++)`` prompt.

.. _`sticky mode`:

``sticky [start end]``
  Toggle **sticky mode**.  When in this mode, every time the current position
  changes, the screen is repainted and the whole function shown.  Thus, when
  doing step-by-step execution you can easily follow the flow of the
  execution.  If ``start`` and ``end`` are given, sticky mode is enabled and
  only lines within that range (extremes included) will be displayed.


``longlist`` (``ll``)
  List source code for the current function.  Different from the normal pdb
  ``list`` command, ``longlist`` displays the whole function.  The current
  line is marked with ``->``.  In case of post-mortem debugging, the line
  which actually raised the exception is marked with ``>>``.  If the
  ``highlight`` `config option`_ is set and Pygments_ is installed, the source
  code is highlighted.


``interact``
  Start an interactive interpreter whose global namespace contains all the
  names found in the current scope.


``display EXPRESSION``
  Add an expression to the **display list**; expressions in this list are
  evaluated at each step, and printed every time its value changes.
  **WARNING**: since these expressions are evaluated multiple times, make sure
  not to put expressions with side-effects in the display list.

``undisplay EXPRESSION``:
  Remove ``EXPRESSION`` from the display list.

``source EXPRESSION``
  Show the source code for the given function/method/class.

``edit EXPRESSION``
  Open the editor in the right position to edit the given
  function/method/class.  The editor used is specified in a `config
  option`_.

``hf_unhide``, ``hf_hide``, ``hf_list``
  Some frames might be marked as "hidden" by e.g. using the `@pdb.hideframe`_
  function decorator.  By default, hidden frames are not shown in the stack
  trace, and cannot be reached using ``up`` and ``down``.  You can use
  ``hf_unhide`` to tell pdb to ignore the hidden status (i.e., to treat hidden
  frames as normal ones), and ``hf_hide`` to hide them again.  ``hf_list``
  prints a list of hidden frames.
  The config option ``enable_hidden_frames`` can be used to disable handling
  of hidden frames in general.


Smart command parsing
---------------------

By default, pdb tries hard to interpret what you enter at the command prompt
as one of its builtin commands.  However, this is inconvenient if you want to
just print the value of a local variable which happens to have the same name
as one of the commands. E.g.::

    (Pdb) list
      1
      2     def fn():
      3         c = 42
      4         import pdb;pdb.set_trace()
      5  ->     return c
    (Pdb) c

In the example above, instead of printing 42 pdb interprets the input as the
command ``continue``, and then you lose your prompt.  It's even worse than
that, because it happens even if you type e.g. ``c.__class__``.

pdb++ fixes this unfriendly (from the author's point of view, of course :-))
behavior by always preferring the variable in scope, if it exists.  If you really
want to execute the corresponding command, you can prefix it with ``!!``.
Thus, the example above becomes::

    (Pdb++) list
      1
      2     def fn():
      3         c = 42
      4         import pdb;pdb.set_trace()
      5  ->     return c
    (Pdb++) c
    42
    (Pdb++) !!c

Note that the "smart" behavior takes place only when there is ambiguity, i.e.
if there exists a variable with the same name as a command: in all other
cases, everything works as usual.

Regarding the ``list`` command itself, using ``list(…`` is a special case
that gets handled as the Python builtin::

    (Pdb++) list([1, 2])
    [1, 2]

Additional functions in the ``pdb`` module
------------------------------------------

The ``pdb`` module that comes with pdb++ includes all the functions and
classes that are in the module from the standard library.  If you find any
difference, please report it as a bug.

In addition, there are some new convenience functions that are unique to
pdb++.

``pdb.xpm()``
  eXtended Post Mortem: it is equivalent to
  ``pdb.post_mortem(sys.exc_info()[2])``.  If used inside an ``except``
  clause, it will start a post-mortem pdb prompt from the line that raised the
  exception being caught.

``pdb.disable()``
  Disable ``pdb.set_trace()``: any subsequent call to it will be ignored.

``pdb.enable()``
  Re-enable ``pdb.set_trace()``, in case it was disabled by ``pdb.disable()``.

.. _`@pdb.hideframe`:

``@pdb.hideframe``
  A function decorator that tells pdb++ to hide the frame corresponding to the
  function.  Hidden frames do not show up when using interactive commands such
  as ``up``, ``down`` or ``where``, unless ``hf_unhide`` is invoked.

``@pdb.break_on_setattr(attrname, condition=always)``
  class decorator: break the execution of the program every time the
  attribute ``attrname`` is set on any instance of the class. ``condition`` is
  a callable that takes the target object of the ``setattr`` and the actual value;
  by default, it breaks every time the attribute is set. E.g.::

      @break_on_setattr('bar')
      class Foo:
          pass
      f = Foo()
      f.bar = 42    # the program breaks here

  If can be used even after the class has already been created, e.g. if we
  want to break when some attribute of a particular object is set::

      class Foo:
          pass
      a = Foo()
      b = Foo()

      def break_if_a(obj, value):
          return obj is a

      break_on_setattr('bar', condition=break_if_a)(Foo)
      b.bar = 10   # no break
      a.bar = 42   # the program breaks here

  This can be used after ``pdb.set_trace()`` also::

      (Pdb++) import pdb
      (Pdb++) pdb.break_on_setattr('tree_id')(obj.__class__)
      (Pdb++) continue


Configuration and customization
-------------------------------

.. _`config option`:

To customize pdb++, you can put a file named ``.pdbrc.py`` in your home
directory.  The file must contain a class named ``Config`` inheriting from
``pdb.DefaultConfig`` and override the desired values.

The following is a list of the options you can customize, together with their
default value:

``prompt = '(Pdb++) '``
  The prompt to show when in interactive mode.

``highlight = True``
  Highlight line numbers and the current line when showing the ``longlist`` of
  a function or when in **sticky mode**.

``encoding = 'utf-8'``
  File encoding. Useful when there are international characters in your string
  literals or comments.

``sticky_by_default = False``
  Determine whether pdb++ starts in sticky mode or not.

``line_number_color = pdb.Color.turquoise``
  The color to use for line numbers.
  See `Notes on color options`_.

``filename_color = pdb.Color.yellow``
  The color to use for file names when printing the stack entries.
  See `Notes on color options`_.

``current_line_color = "39;49;7"``
  The SGR parameters for the ANSI escape sequence to highlight the current
  line.  The default uses the default foreground (``39``) and background
  (``49``) colors, inversed (``7``).
  See `Notes on color options`_.

``editor = None``
  The command to invoke when using the ``edit`` command. By default, it uses ``$EDITOR``
  if set, else ``vim`` or ``vi`` (if found).  If only the editor command is specified, the ``emacs`` and
  ``vi`` notation will be used to specify the line number: ``COMMAND +n filename``. It's
  otherwise possible to use another syntax by using the placeholders ``{filename}`` and
  ``{lineno}``. For example with sublime text, specify ``editor = "subl
  {filename}:{lineno}"``.

``truncate_long_lines = True``
  Truncate lines which exceed the terminal width.

``enable_hidden_frames = True``
  Certain frames can be hidden by default.
  If enabled, the commands ``hf_unhide``, ``hf_hide``, and ``hf_list`` can be
  used to control display of them.

``show_hidden_frames_count = True``
  If ``enable_hidden_frames`` is ``True`` this controls if the number of
  hidden frames gets displayed.

``def setup(self, pdb): pass``
  This method is called during the initialization of the ``Pdb`` class. Useful
  to do complex setup.

``show_traceback_on_error = True``
  Display tracebacks for errors via ``Pdb.error``, that come from
  ``Pdb.default`` (i.e. the execution of an unrecognized pdb command),
  and are not a direct cause of the expression itself (e.g. ``NameError``
  with a command like ``doesnotexist``).

  With this option disabled only ``*** exception string`` gets printed, which
  often misses useful context.

``show_traceback_on_error_limit = None``
  This option sets the limit to be used with ``traceback.format_exception``,
  when ``show_traceback_on_error`` is enabled.

Options relevant for source code highlighting (using Pygments)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``use_pygments = None``
  By default Pygments_ is used for syntax highlighting of source code when it
  can be imported, e.g. when showing the ``longlist`` of a function or when in
  **sticky mode**.

``pygments_formatter_class = None``

  You can configure the Pygments formatter to use via the
  ``pygments_formatter_class`` config setting as a string (dotted path).
  This should be one of the following typically:
  ``"pygments.formatters.Terminal256Formatter"``,
  ``"pygments.formatters.TerminalTrueColorFormatter"``, or
  ``"pygments.formatters.TerminalFormatter"``.

  The default is to auto-detect the best formatter based on the ``$TERM``
  variable, e.g. it uses ``Terminal256Formatter`` if the ``$TERM`` variable
  contains "256color" (e.g. ``xterm-256color``), but also knows about
  e.g. "xterm-kitty" to support true colors (``TerminalTrueColorFormatter``).
  ``TerminalFormatter`` gets used as a fallback.

``pygments_formatter_kwargs = {}``

  A dictionary of keyword arguments to pass to the formatter's constructor.

  The default arguments (updated with this setting) are::

      {
          "style": "default",
          "bg": self.config.bg,
          "colorscheme": self.config.colorscheme,
      }

    ``style = 'default'``

     The style to use, can be a string or a Pygments Style subclass.
     E.g. ``"solarized-dark"``.
     See http://pygments.org/docs/styles/.

   ``bg = 'dark'``

     Selects a different palette for dark/light backgrounds.
     Only used by ``TerminalFormatter``.

   ``colorscheme = None``

     A dictionary mapping token types to (lightbg, darkbg) color names or
     ``None`` (default: ``None`` = use builtin colorscheme).
     Only used by ``TerminalFormatter``.

Example::

    class Config(pdb.DefaultConfig):
        pygments_formatter_class = "pygments.formatters.TerminalTrueColorFormatter"
        pygments_formatter_kwargs = {"style": "solarized-light"}

.. _SGR parameters: https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters

Notes on color options
^^^^^^^^^^^^^^^^^^^^^^

The values for color options will be used inside of the SGR escape sequence
``\e[%sm`` where ``\e`` is the ESC character and ``%s`` the given value.
See `SGR parameters`_.

The following means "reset all colors" (``0``), set foreground color to 18
(``48;5;18``), and background to ``21``: ``"0;48;5;18;38;5;21"``.

Constants are available via ``pdb.Color``, e.g. ``pdb.Color.red``
(``"31;01"``), but in general any string can be used here.

Coding guidelines
-----------------

``pdb++`` is developed using Test Driven Development, and we try to keep test
coverage high.

As a general rule, every commit should come with its own test. If it's a new
feature, it should come with one or many tests which exercise it. If it's a
bug fix, the test should **fail before the fix**, and pass after.

The goal is to make refactoring easier in the future: if you wonder why a
certain line of code does something, in principle it should be possible to
comment it out and see which tests fail.

In exceptional cases, the test might be too hard or impossible to write: in
such cases it is fine to do a commit without a test, but you should explain
very precisely in the commit message why it is hard to write a test and how to
reproduce the buggy behavior by hand.

It is fine NOT to write a test in the following cases:

  - typos, documentation, and in general any non-coding commit

  - code refactorings which do not add any feature

  - commits which fix an already failing test

  - commits to silence warnings

  - purely cosmetic changes, such as change the color of the output

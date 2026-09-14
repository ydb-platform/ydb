=============================================
Shellingham: Tool to Detect Surrounding Shell
=============================================

.. image:: https://img.shields.io/pypi/v/shellingham.svg
    :target: https://pypi.org/project/shellingham/

Shellingham detects what shell the current Python executable is running in.


Usage
=====

.. code-block:: python

    >>> import shellingham
    >>> shellingham.detect_shell()
    ('bash', '/bin/bash')

``detect_shell`` pokes around the process's running environment to determine
what shell it is run in. It returns a 2-tuple:

* The shell name, always lowercased.
* The command used to run the shell.

``ShellDetectionFailure`` is raised if ``detect_shell`` fails to detect the
surrounding shell.


Notes
=====

* The shell name is always lowercased.
* On Windows, the shell name is the name of the executable, minus the file
  extension.


Notes for Application Developers
================================

Remember, your application's user is not necessarily using a shell.
Shellingham raises ``ShellDetectionFailure`` if there is no shell to detect,
but *your application should almost never do this to your user*.

A practical approach to this is to wrap ``detect_shell`` in a try block, and
provide a sane default on failure

.. code-block:: python

    try:
        shell = shellingham.detect_shell()
    except shellingham.ShellDetectionFailure:
        shell = provide_default()


There are a few choices for you to choose from.

* The POSIX standard mandates the environment variable ``SHELL`` to refer to
  "the user's preferred command language interpreter". This is always available
  (even if the user is not in an interactive session), and likely the correct
  choice to launch an interactive sub-shell with.
* A command ``sh`` is almost guaranteed to exist, likely at ``/bin/sh``, since
  several POSIX tools rely on it. This should be suitable if you want to run a
  (possibly non-interactive) script.
* All versions of DOS and Windows have an environment variable ``COMSPEC``.
  This can always be used to launch a usable command prompt (e.g. `cmd.exe` on
  Windows).

Here's a simple implementation to provide a default shell

.. code-block:: python

    import os

    def provide_default():
        if os.name == 'posix':
            return os.environ['SHELL']
        elif os.name == 'nt':
            return os.environ['COMSPEC']
        raise NotImplementedError(f'OS {os.name!r} support not available')

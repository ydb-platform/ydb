spur.py: Run commands and manipulate files locally or over SSH using the same interface
=======================================================================================

To run echo locally:

.. code-block:: python

    import spur

    shell = spur.LocalShell()
    result = shell.run(["echo", "-n", "hello"])
    print(result.output) # prints hello

Executing the same command over SSH uses the same interface -- the only
difference is how the shell is created:

.. code-block:: python

    import spur

    shell = spur.SshShell(hostname="localhost", username="bob", password="password1")
    with shell:
        result = shell.run(["echo", "-n", "hello"])
    print(result.output) # prints hello

Installation
------------

``$ pip install spur``

Shell constructors
------------------

LocalShell
~~~~~~~~~~

Takes no arguments:

.. code-block:: python

    spur.LocalShell()

SshShell
~~~~~~~~

Requires a hostname. Also requires some combination of a username,
password and private key, as necessary to authenticate:

.. code-block:: python

    # Use a password
    spur.SshShell(
        hostname="localhost",
        username="bob",
        password="password1"
    )
    # Use a private key
    spur.SshShell(
        hostname="localhost",
        username="bob",
        private_key_file="path/to/private.key"
    )
    # Use a port other than 22
    spur.SshShell(
        hostname="localhost",
        port=50022,
        username="bob",
        password="password1"
    )

Optional arguments:

* ``connect_timeout`` -- a timeout in seconds for establishing an SSH
  connection. Defaults to 60 (one minute).

* ``missing_host_key`` -- by default, an error is raised when a host
  key is missing. One of the following values can be used to change the
  behaviour when a host key is missing:

  - ``spur.ssh.MissingHostKey.raise_error`` -- raise an error
  - ``spur.ssh.MissingHostKey.warn`` -- accept the host key and log a
    warning
  - ``spur.ssh.MissingHostKey.accept`` -- accept the host key

* ``shell_type`` -- the type of shell used by the host. Defaults to
  ``spur.ssh.ShellTypes.sh``, which should be appropriate for most Linux
  distributions. If the host uses a different shell, such as simpler shells
  often found on embedded systems, try changing ``shell_type`` to a more
  appropriate value, such as ``spur.ssh.ShellTypes.minimal``. The following
  shell types are currently supported:

  - ``spur.ssh.ShellTypes.sh`` -- the Bourne shell. Supports all features.

  - ``spur.ssh.ShellTypes.minimal`` -- a minimal shell. Several features
    are unsupported:

    - Non-existent commands will not raise ``spur.NoSuchCommandError``.

    - The following arguments to ``spawn`` and ``run`` are unsupported unless
      set to their default values:
      ``cwd``, ``update_env``, and ``store_pid``.

* ``look_for_private_keys`` -- by default, Spur will search for discoverable
  private key files in ``~/.ssh/``.
  Set to ``False`` to disable this behaviour.

* ``load_system_host_keys`` -- by default, Spur will attempt to read host keys
  from the user's known hosts file, as used by OpenSSH, and no exception will
  be raised if the file can't be read.
  Set to ``False`` to disable this behaviour.

* ``sock`` -- an open socket or socket-like object to use for communication to
  the target host. For instance:

  .. code-block:: python

      sock=paramiko.proxy.ProxyCommand(
          "ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
          "bob@proxy.example.com nc -q0 target.example.com 22"
      )

  Examples of socket-like objects include:

  * |paramiko.Channel|_
  * |paramiko.proxy.ProxyCommand|_
    (`unsupported in Python 3 <https://github.com/paramiko/paramiko/issues/673>`_ as of writing)

.. |paramiko.Channel| replace:: ``paramiko.Channel``
.. _paramiko.Channel: http://docs.paramiko.org/en/latest/api/channel.html

.. |paramiko.proxy.ProxyCommand| replace:: ``paramiko.proxy.ProxyCommand``
.. _paramiko.proxy.ProxyCommand: http://docs.paramiko.org/en/latest/api/proxy.html

Shell interface
---------------

run(command, cwd, update\_env, store\_pid, allow\_error, stdout, stderr, encoding)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run a command and wait for it to complete. The command is expected to be
a list of strings. Returns an instance of ``ExecutionResult``.

.. code-block:: python

    result = shell.run(["echo", "-n", "hello"])
    print(result.output) # prints hello

Note that arguments are passed without any shell expansion. For
instance, ``shell.run(["echo", "$PATH"])`` will print the literal string
``$PATH`` rather than the value of the environment variable ``$PATH``.

Raises ``spur.NoSuchCommandError`` if trying to execute a non-existent
command.

Raises ``spur.CouldNotChangeDirectoryError`` if changing the current directory
to ``cwd`` failed.

Optional arguments:

* ``cwd`` -- change the current directory to this value before
  executing the command.
* ``update_env`` -- a ``dict`` containing environment variables to be
  set before running the command. If there's an existing environment
  variable with the same name, it will be overwritten. Otherwise, it is
  unchanged.
* ``store_pid`` -- if set to ``True`` when calling ``spawn``, store the
  process id of the spawned process as the attribute ``pid`` on the
  returned process object. Has no effect when calling ``run``.
* ``allow_error`` -- ``False`` by default. If ``False``, an exception
  is raised if the return code of the command is anything but 0. If
  ``True``, a result is returned irrespective of return code.
* ``stdout`` -- if not ``None``, anything the command prints to
  standard output during its execution will also be written to
  ``stdout`` using ``stdout.write``.
* ``stderr`` -- if not ``None``, anything the command prints to
  standard error during its execution will also be written to
  ``stderr`` using ``stderr.write``.
* ``encoding`` -- if set, this is used to decode any output.
  By default, any output is treated as raw bytes.
  If set, the raw bytes are decoded before writing to
  the passed ``stdout`` and ``stderr`` arguments (if set)
  and before setting the output attributes on the result.

``shell.run(*args, **kwargs)`` should behave similarly to
``shell.spawn(*args, **kwargs).wait_for_result()``

spawn(command, cwd, update\_env, store\_pid, allow\_error, stdout, stderr, encoding)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Behaves the same as ``run`` except that ``spawn`` immediately returns an
object representing the running process.

Raises ``spur.NoSuchCommandError`` if trying to execute a non-existent
command.

Raises ``spur.CouldNotChangeDirectoryError`` if changing the current directory
to ``cwd`` failed.

open(path, mode="r")
~~~~~~~~~~~~~~~~~~~~

Open the file at ``path``. Returns a file-like object.

By default, files are opened in text mode.
Appending `"b"` to the mode will open the file in binary mode.

For instance, to copy a binary file over SSH,
assuming you already have an instance of ``SshShell``:

.. code-block:: python

    with ssh_shell.open("/path/to/remote", "rb") as remote_file:
        with open("/path/to/local", "wb") as local_file:
            shutil.copyfileobj(remote_file, local_file)

close()
~~~~~~~

Closes and the shell and releases any associated resources.
``close()`` is called automatically when the shell is used as a context manager.

Process interface
-----------------

Returned by calls to ``shell.spawn``. Has the following attributes:

* ``pid`` -- the process ID of the process. Only available if
  ``store_pid`` was set to ``True`` when calling ``spawn``.

Has the following methods:

* ``is_running()`` -- return ``True`` if the process is still running,
  ``False`` otherwise.
* ``stdin_write(value)`` -- write ``value`` to the standard input of
  the process.
* ``wait_for_result()`` -- wait for the process to exit, and then
  return an instance of ``ExecutionResult``. Will raise
  ``RunProcessError`` if the return code is not zero and
  ``shell.spawn`` was not called with ``allow_error=True``.
* ``send_signal(signal)`` -- sends the process the signal ``signal``.
  Only available if ``store_pid`` was set to ``True`` when calling
  ``spawn``.

Classes
-------

ExecutionResult
~~~~~~~~~~~~~~~

``ExecutionResult`` has the following properties:

* ``return_code`` -- the return code of the command
* ``output`` -- a string containing the result of capturing stdout
* ``stderr_output`` -- a string containing the result of capturing
  stdout

It also has the following methods:

* ``to_error()`` -- return the corresponding RunProcessError. This is
  useful if you want to conditionally raise RunProcessError, for
  instance:

.. code-block:: python

    result = shell.run(["some-command"], allow_error=True)
    if result.return_code > 4:
        raise result.to_error()

RunProcessError
~~~~~~~~~~~~~~~

A subclass of ``RuntimeError`` with the same properties as
``ExecutionResult``:

* ``return_code`` -- the return code of the command
* ``output`` -- a string containing the result of capturing stdout
* ``stderr_output`` -- a string containing the result of capturing
  stdout

NoSuchCommandError
~~~~~~~~~~~~~~~~~~

``NoSuchCommandError`` has the following properties:

* ``command`` -- the command that could not be found

CouldNotChangeDirectoryError
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``CouldNotChangeDirectoryError`` has the following properties:

* ``directory`` -- the directory that could not be changed to

API stability
-------------

Using the the terminology from `Semantic
Versioning <http://semver.org/spec/v1.0.0.html>`_, if the version of
spur is X.Y.Z, then X is the major version, Y is the minor version, and
Z is the patch version.

While the major version is 0, incrementing the patch version indicates a
backwards compatible change. For instance, if you're using 0.3.1, then
it should be safe to upgrade to 0.3.2.

Incrementing the minor version indicates a change in the API. This means
that any code using previous minor versions of spur may need updating
before it can use the current minor version.

Undocumented features
~~~~~~~~~~~~~~~~~~~~~

Some features are undocumented, and should be considered experimental.
Use them at your own risk. They may not behave correctly, and their
behaviour and interface may change at any time.

Troubleshooting
---------------

I get the error "Connection refused" when trying to connect to a virtual machine using a forwarded port on ``localhost``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Try using ``"127.0.0.1"`` instead of ``"localhost"`` as the hostname.

I get the error "Connection refused" when trying to execute commands over SSH
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Try connecting to the machine using SSH on the command line with the
same settings. For instance, if you're using the code:

.. code-block:: python

    shell = spur.SshShell(
            hostname="remote",
            port=2222,
            username="bob",
            private_key_file="/home/bob/.ssh/id_rsa"
        )
    with shell:
        result = shell.run(["echo", "hello"])

Try running:

.. code-block:: sh

    ssh bob@remote -p 2222 -i /home/bob/.ssh/id_rsa

If the ``ssh`` command succeeds, make sure that the arguments to
``ssh.SshShell`` and the ``ssh`` command are the same. If any of the
arguments to ``ssh.SshShell`` are dynamically generated, try hard-coding
them to make sure they're set to the values you expect.

I can't spawn or run commands over SSH
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're having trouble spawning or running commands over SSH, try passing
``shell_type=spur.ssh.ShellTypes.minimal`` as an argument to ``spur.SshShell``.
For instance:

.. code-block:: python

    import spur
    import spur.ssh

    spur.SshShell(
        hostname="localhost",
        username="bob",
        password="password1",
        shell_type=spur.ssh.ShellTypes.minimal,
    )

This makes minimal assumptions about the features that the host shell supports,
and is especially well-suited to minimal shells found on embedded systems. If
the host shell is more fully-featured but only works with
``spur.ssh.ShellTypes.minimal``, feel free to submit an issue.

Why don't shell features such as variables and redirection work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Commands are run directly rather than through a shell.
If you want to use any shell features such as variables and redirection,
then you'll need to run those commands within an appropriate shell.
For instance:

.. code-block:: python

    shell.run(["sh", "-c", "echo $PATH"])
    shell.run(["sh", "-c", "ls | grep bananas"])

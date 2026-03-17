"""
    Testing tools for running cmdline methods
"""
import sys
import os
import importlib.util
import logging
from functools import update_wrapper
import inspect
import textwrap
from contextlib import closing
import subprocess

try:
    from unittest.mock import patch
except ImportError:
    # python 2
    from mock import patch

import execnet
from six.moves import cPickle  # @UnresolvedImport

from . import cmdline

try:
    # Python 3
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack

try:  # Python 2
    str_type = basestring
except NameError:  # Python 3
    str_type = str


log = logging.getLogger(__name__)


# TODO: add option to return results as a pipe to avoid buffering
#       large amounts of output
def run(cmd, stdin=None, capture_stdout=True, capture_stderr=False,
        check_rc=True, background=False, **kwargs):
    """
    Run a command; raises `subprocess.CalledProcessError` on failure.

    Parameters
    ----------
    stdin : file object
        text piped to standard input
    capture_stdout : `bool` or `stream`
        If set, stdout will be captured and returned
    capture_stderr : `bool`
        If set, stderr will be piped to stdout and returned
    **kwargs : optional arguments
        Other arguments are passed to Popen()
    """
    log.debug('exec: %s' % str(cmd))

    stdout = subprocess.PIPE if capture_stdout is True else capture_stdout if capture_stdout else None
    stderr = subprocess.STDOUT if capture_stderr else None
    stdin_arg = None if stdin is None else subprocess.PIPE

    p = subprocess.Popen(cmd, stdin=stdin_arg, stdout=stdout, stderr=stderr, **kwargs)

    if background:
        return p

    (out, _) = p.communicate(stdin)

    if out is not None and not isinstance(out, str_type):
        try:
            out = out.decode('utf-8')
        except:
            log.warning("Unable to decode command output to UTF-8")

    if check_rc and p.returncode != 0:
        err_msg = ((out if out else 'No output') if capture_stdout is True
                   else '<not captured>')
        cmd = cmd if isinstance(cmd, str) else ' '.join(cmd)
        log.error("Command failed: \"%s\"\n%s" % (cmd, err_msg.strip()))
        ex = subprocess.CalledProcessError(p.returncode, cmd)
        ex.output = err_msg
        raise ex

    return out


def run_as_main(fn, *argv):
    """ Run a given function as if it was the system entry point,
        eg for testing scripts.

    Eg::

        from scripts.Foo import main

        run_as_main(main, 'foo','bar')

    This is equivalent to ``Foo foo bar``, assuming
    ``scripts.Foo.main`` is registered as an entry point.
    """
    with patch("sys.argv", new=['progname'] + list(argv)):
        log.info("run_as_main: %s" % str(argv))
        return fn()


def run_module_as_main(module, argv=[]):
    """ Run a given module as if it was the system entry point.
    """
    where = os.path.dirname(module.__file__)
    filename = os.path.basename(module.__file__)
    filename = os.path.splitext(filename)[0] + ".py"

    with patch("sys.argv", new=argv):
        spec = importlib.util.spec_from_file_location(
            "__main__", os.path.join(where, filename))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


def _evaluate_fn_source(src, *args, **kwargs):
    locals_ = {}
    eval(compile(src, '<string>', 'single'), {}, locals_)
    fn = next(iter(locals_.values()))
    if isinstance(fn, staticmethod):
        fn = fn.__get__(None, object)
    return fn(*args, **kwargs)


def _invoke_method(obj, name, *args, **kwargs):
    return getattr(obj, name)(*args, **kwargs)


def _find_class_from_staticmethod(fn):
    for _, cls in inspect.getmembers(sys.modules[fn.__module__], inspect.isclass):
        for name, member in inspect.getmembers(cls):
            if member is fn or (isinstance(member, staticmethod) and member.__get__(None, object) is fn):
                return cls, name
    return None, None


def _make_pickleable(fn):
    # return a pickleable function followed by a tuple of initial arguments
    # could use partial but this is more efficient
    try:
        cPickle.dumps(fn, protocol=0)
    except (TypeError, cPickle.PickleError, AttributeError):
        pass
    else:
        return fn, ()
    if inspect.ismethod(fn):
        name, self_ = fn.__name__, fn.__self__
        if self_ is None:  # Python 2 unbound method
            self_ = fn.im_class
        return _invoke_method, (self_, name)
    elif inspect.isfunction(fn) and fn.__module__ in sys.modules:
        cls, name = _find_class_from_staticmethod(fn)
        if (cls, name) != (None, None):
            try:
                cPickle.dumps((cls, name), protocol=0)
            except cPickle.PicklingError:
                pass
            else:
                return _invoke_method, (cls, name)
    # Fall back to sending the source code
    return _evaluate_fn_source, (textwrap.dedent(inspect.getsource(fn)),)


def _run_in_subprocess_redirect_stdout(fd):
    import os  # @Reimport
    import sys  # @Reimport
    sys.stdout.close()
    os.dup2(fd, 1)
    os.close(fd)
    sys.stdout = os.fdopen(1, 'w', 1)


def _run_in_subprocess_remote_fn(channel):
    from six.moves import cPickle  # @UnresolvedImport @Reimport # NOQA
    fn, args, kwargs = cPickle.loads(channel.receive(None))
    channel.send(cPickle.dumps(fn(*args, **kwargs), protocol=0))


def run_in_subprocess(fn, python=sys.executable, cd=None, timeout=None):
    """ Wrap a function to run in a subprocess.  The function must be
        pickleable or otherwise must be totally self-contained; it must not
        reference a closure or any globals.  It can also be the source of a
        function (def fn(...): ...).

        Raises execnet.RemoteError on exception.
    """
    pkl_fn, preargs = (_evaluate_fn_source, (fn,)) if isinstance(fn, str) else _make_pickleable(fn)
    spec = '//'.join(filter(None, ['popen', 'python=' + python, 'chdir=' + str(cd) if cd else None]))

    def inner(*args, **kwargs):
        # execnet sends stdout to /dev/null :(
        fix_stdout = sys.version_info < (3, 0, 0)  # Python 3 passes close_fds=True to subprocess.Popen
        with ExitStack() as stack:
            with ExitStack() as stack2:
                if fix_stdout:
                    fd = os.dup(1)
                    stack2.callback(os.close, fd)
                gw = execnet.makegateway(spec)  # @UndefinedVariable
                stack.callback(gw.exit)
            if fix_stdout:
                with closing(gw.remote_exec(_run_in_subprocess_remote_fn)) as chan:
                    chan.send(cPickle.dumps((_run_in_subprocess_redirect_stdout, (fd,), {}), protocol=0))
                    chan.receive(None)
            with closing(gw.remote_exec(_run_in_subprocess_remote_fn)) as chan:
                payload = (pkl_fn, tuple(i for t in (preargs, args) for i in t), kwargs)
                chan.send(cPickle.dumps(payload, protocol=0))
                return cPickle.loads(chan.receive(timeout))
    return inner if isinstance(fn, str) else update_wrapper(inner, fn)


def run_with_coverage(cmd, pytestconfig, coverage=None, cd=None, **kwargs):
    """
    Run a given command with coverage enabled. This won't make any sense
    if the command isn't a python script.

    This must be run within a pytest session that has been setup with
    the '--cov=xxx' options, and therefore requires the pytestconfig
    argument that can be retrieved from the standard pytest funcarg
    of the same name.

    Parameters
    ----------
    cmd: `List`
        Command to run
    pytestconfig: `pytest._config.Config`
        Pytest configuration object
    coverage: `str`
        Path to the coverage executable
    cd: `str`
        If not None, will change to this directory before running the cmd.
        This is the directory that the coverage files will be created in.
    kwargs: keyword arguments
        Any extra arguments to pass to `pkglib.cmdline.run`

    Returns
    -------
    `str` standard output

    Examples
    --------

    >>> def test_example(pytestconfig):
    ...   cmd = ['python','myscript.py']
    ...   run_with_coverage(cmd, pytestconfig)
    """
    if isinstance(cmd, str):
        cmd = [cmd]

    if coverage is None:
        coverage = [sys.executable, '-mcoverage.__main__']
    elif isinstance(coverage, str):
        coverage = [coverage]

    args = coverage + ['run', '-p']
    if getattr(pytestconfig.option, 'cov_source', None):
        source_dirs = ",".join(pytestconfig.option.cov_source)
        args += ['--source=%s' % source_dirs]
    args += cmd
    if cd:
        with cmdline.chdir(cd):
            return run(args, **kwargs)
    return run(args, **kwargs)

"""Cmdline tools utility module
"""
import logging
import getpass
import os
import shutil
import sys

from contextlib import contextmanager
from tempfile import mkdtemp


try:  # Python 2
    str_type = basestring
except NameError:  # Python 3
    str_type = str


def get_log():
    return logging.getLogger(__name__)


@contextmanager
def umask(new_mask):
    """
    Context Manager to set the umask
    """
    try:
        old_mask = os.umask(new_mask)
        yield
    finally:
        os.umask(old_mask)


@contextmanager
def chdir(dirname):
    """
    Context Manager to change to a dir then change back
    """
    try:
        here = os.getcwd()
    except OSError:
        get_log().warning("CWD has gone away, will chdir to back to '/'")
        here = '/'
    try:
        os.chdir(dirname)
        yield
    finally:
        os.chdir(here)


class PrettyFormatter(object):
    def __init__(self, color=True):
        from termcolor import colored
        if color is False:
            os.environ["FORCE_COLOR"] = "true"
        self.color = color
        self.colored = colored
        self.buffer = []

    def hr(self):
        if self.color:
            self.buffer.append(self.colored("-" * 80, 'blue', attrs=['bold']))
        else:
            self.buffer.append("-" * 80)

    def title(self, txt):
        if self.color:
            self.buffer.append(self.colored("  %s" % txt, 'blue', attrs=['bold']))
        else:
            self.buffer.append("  %s" % txt)

    def p(self, txt, color, attrs=[]):
        if self.color:
            self.buffer.append(self.colored(txt, color, attrs=attrs))
        else:
            self.buffer.append(txt)

    def flush(self):
        print(self.__str__())
        self.buffer = []

    def __str__(self):
        return '\n'.join(self.buffer)


class TempDir(object):
    """Context manager for a temporary directory.

    Examples
    --------

    >>> import os
    >>> from pkgutils.cmdline import TempDir
    >>> with TempDir() as dir:
    ...   print(os.path.exists(dir))
    True
    >>> os.path.exists(dir)
    False
    """
    def __init__(self, delete=True, temp_dir=None, force_dir=None):
        self.delete = delete
        self.created = False

        if force_dir:
            if temp_dir:
                raise RuntimeError("Either `temp_dir` or `force_dir` can be provided, not both")

            self.dir = force_dir
        else:
            if temp_dir and not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            self.dir = mkdtemp(dir=temp_dir)
            self.created = True

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
            self.created = True

        if self.created:
            get_log().info("Created tempdir at %s" % self.dir)

    def close(self):
        """Delete the directory"""
        if self.delete and self.dir is not None:
            try:
                get_log().info('Deleting %s' % self.dir)
                shutil.rmtree(self.dir)
            except Exception as e:
                get_log().error('could not delete %s - %s' % (self.dir, e[0]))
            finally:
                self.dir = None

    def __enter__(self):
        return self.dir

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


def copy_files(src, dest):
    """Copies files from one directory to another"""
    src_files = os.listdir(src)
    for file_name in src_files:
        full_file_name = os.path.join(src, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest)


class _Getch:
    """ Gets a single character from standard input.  Does not echo to the screen."""
    def __init__(self):
        try:
            self.impl = _GetchWindows()
        except ImportError:
            self.impl = _GetchUnix()

    def __call__(self):
        return self.impl()


class _GetchUnix:
    def __init__(self):
        import tty  # @UnusedImport # NOQA
        import sys  # @UnusedImport # NOQA

    def __call__(self):
        import sys
        import tty
        import termios
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch


class _GetchWindows:
    def __init__(self):
        import msvcrt  # @UnusedImport # NOQA

    def __call__(self):
        import msvcrt
        return msvcrt.getch()  # @UndefinedVariable

getch = _Getch()


def request_username():
    """Prompt for user name"""
    default_user = getpass.getuser()
    username = raw_input('Username (default: %s): ' % default_user)
    if not username:
        username = default_user
    return username


def request_password():
    """Prompt for password"""
    password = ''
    while not password:
        password = getpass.getpass()
    return password


def wait_for_user_confirmation(text="Press ENTER to continue.."):
    getpass.getpass(text)


def which(name, flags=os.X_OK):
    """Analogue of unix 'which'. Borrowed from the Twisted project, see
       their licence here: https://twistedmatrix.com/trac/browser/trunk/LICENSE
    """
    result = []
    exts = filter(None, os.environ.get('PATHEXT', '').split(os.pathsep))
    path = os.environ.get('PATH', None)
    if path is None:
        return []
    for p in os.environ.get('PATH', '').split(os.pathsep):
        p = os.path.join(p, name)
        if os.access(p, flags):
            result.append(p)
        for e in exts:
            pext = p + e
            if os.access(pext, flags):
                result.append(pext)
    return result


def get_real_python_executable():
    real_sys_executable = os.path.realpath(sys.executable)
    real_prefix = getattr(sys, "real_prefix", None)
    if not real_prefix:
        return real_sys_executable

    executable_name = os.path.basename(real_sys_executable)
    bindir = os.path.join(real_prefix, "bin")
    if not os.path.isdir(bindir):
        print("Unable to access bin directory of original Python "
              "installation at: %s" % bindir)
        return real_sys_executable

    executable = os.path.join(bindir, executable_name)
    if not os.path.exists(executable):
        executable = None
        for f in os.listdir(bindir):
            if not f.endswith("ython"):
                continue

            f = os.path.join(bindir, f)
            if os.path.isfile(f):
                executable = f
                break

        if not executable:
            print("Unable to locate a valid Python executable of original "
                  "Python installation at: %s" % bindir)
            executable = real_sys_executable

    return executable

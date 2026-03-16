from pyflakes.checker import Binding, Assignment, Checker
from pyflakes.api import isPythonFile
import _ast
import re
import pathlib
import pytest
import sys
import tokenize


PYTEST_GTE_7 = hasattr(pytest, 'version_tuple') and pytest.version_tuple >= (7, 0)


def assignment_monkeypatched_init(self, name, source):
    Binding.__init__(self, name, source)
    if name == '__tracebackhide__':
        self.used = True

Assignment.__init__ = assignment_monkeypatched_init


HISTKEY = "flakes/mtimes"


def pytest_addoption(parser):
    group = parser.getgroup("general")
    group.addoption(
        '--flakes', action='store_true',
        help="run pyflakes on .py files")
    parser.addini(
        "flakes-ignore", type="linelist",
        help="each line specifies a glob pattern and whitespace "
             "separated pyflakes errors which will be ignored, "
             "example: *.py UnusedImport")


def pytest_configure(config):
    if config.option.flakes:
        config._flakes = FlakesPlugin(config)
        config.pluginmanager.register(config._flakes)
    config.addinivalue_line('markers', "flakes: Tests which run flake.")


class FlakesPlugin:
    def __init__(self, config):
        self.ignore = Ignorer(config.getini("flakes-ignore"))
        self.mtimes = config.cache.get(HISTKEY, {})

    if PYTEST_GTE_7:
        def pytest_collect_file(self, file_path, parent):
            config = parent.config
            if config.option.flakes and isPythonFile(str(file_path)):
                flakesignore = self.ignore(file_path)
                if flakesignore is not None:
                    return FlakesFile.from_parent(parent,
                                                  path=file_path,
                                                  flakesignore=flakesignore)
    else:
        def pytest_collect_file(self, path, parent):
            config = parent.config
            if config.option.flakes and isPythonFile(path.strpath):
                flakesignore = self.ignore(pathlib.Path(str(path)))
                if flakesignore is not None:
                    return FlakesFile.from_parent(parent,
                                                  fspath=path,
                                                  flakesignore=flakesignore)

    def pytest_sessionfinish(self, session):
        session.config.cache.set(HISTKEY, self.mtimes)


class FlakesError(Exception):
    """ indicates an error during pyflakes checks. """


class FlakesFile(pytest.File):
    def __init__(self, *k,  flakesignore, **kw):
        super().__init__(*k, **kw)
        self.flakesignore = flakesignore

    def collect(self):
        return [FlakesItem.from_parent(self, name="flake-8")]


class FlakesItem(pytest.Item):

    def __init__(self, *k, **kw):
        super().__init__(*k, **kw)
        self.add_marker("flakes")
        self.flakesignore = self.parent.flakesignore

    def setup(self):
        flakesmtimes = self.config._flakes.mtimes
        if PYTEST_GTE_7:
            self._flakesmtime = self.path.stat().st_mtime
        else:
            self._flakesmtime = self.fspath.mtime()
        old = flakesmtimes.get(self.nodeid, 0)
        if old == [self._flakesmtime, self.flakesignore]:
            pytest.skip("file(s) previously passed pyflakes checks")

    def runtest(self):
        if PYTEST_GTE_7:
            found_errors, out = check_file(self.path, self.flakesignore)
        else:
            path = pathlib.Path(str(self.fspath))
            found_errors, out = check_file(path, self.flakesignore)
        if found_errors:
            raise FlakesError("\n".join(out))
        # update mtime only if test passed
        # otherwise failures would not be re-run next time
        self.config._flakes.mtimes[self.nodeid] = [self._flakesmtime, self.flakesignore]

    def repr_failure(self, excinfo):
        if excinfo.errisinstance(FlakesError):
            return excinfo.value.args[0]
        return super().repr_failure(excinfo)

    def reportinfo(self):
        if self.flakesignore:
            ignores = "(ignoring %s)" % " ".join(self.flakesignore)
        else:
            ignores = ""
        if PYTEST_GTE_7:
            return (self.path, -1, "pyflakes-check%s" % ignores)
        else:
            return (self.fspath, -1, "pyflakes-check%s" % ignores)


class Ignorer:
    def __init__(self, ignorelines, coderex=re.compile(r"[EW]\d\d\d")):
        self.ignores = ignores = []
        for line in ignorelines:
            i = line.find("#")
            if i != -1:
                line = line[:i]
            try:
                glob, ign = line.split(None, 1)
            except ValueError:
                glob, ign = None, line
            if glob and coderex.match(glob):
                glob, ign = None, line
            ign = ign.split()
            if "ALL" in ign:
                ign = None
            ignores.append((glob, ign))

    def __call__(self, path):
        l = set()
        for (glob, ignlist) in self.ignores:
            if not glob or path.glob(glob):
                if ignlist is None:
                    return None
                l.update(set(ignlist))
        return sorted(l)


def check_file(path, flakesignore):
    with tokenize.open(str(path)) as f:
        codeString = f.read()
    filename = str(path)
    errors = []
    try:
        tree = compile(codeString, filename, "exec", _ast.PyCF_ONLY_AST)
    except SyntaxError:
        value = sys.exc_info()[1]
        (lineno, offset, text) = value.lineno, value.offset, value.text
        if text is None:
            errors.append("%s: problem decoding source" % filename)
        else:
            line = text.splitlines()[-1]

            if offset is not None:
                offset = offset - (len(text) - len(line))

            msg = '%s:%d: %s' % (filename, lineno, value.args[0])
            msg = "{}\n{}".format(msg, line)

            if offset is not None:
                msg = "{}\n{}".format(msg, "%s^" % (" " * offset))
            errors.append(msg)
        return 1, errors
    else:
        # Okay, it's syntactically valid.  Now check it.
        w = Checker(tree, filename)
        w.messages.sort(key=lambda m: m.lineno)
        lines = codeString.split('\n')
        for warning in w.messages:
            if warning.__class__.__name__ in flakesignore or is_ignored_line(lines[warning.lineno - 1].strip()):
                continue
            errors.append(
                '{}:{}: {}\n{}'.format(
                    warning.filename,
                    warning.lineno,
                    warning.__class__.__name__,
                    warning.message % warning.message_args))
        return len(errors), errors


def is_ignored_line(line):
    if line.endswith('# noqa') or line.endswith('# pragma: no flakes'):
        return True
    return False

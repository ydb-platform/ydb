# coding: utf-8

import os

import coverage.config
import coverage.files
import coverage.misc
import coverage.parser
import coverage.plugin
import coverage.python

from build.plugins.lib import test_const
from library.python.testing import coverage_utils


SKIP_FILENAME = '__SKIP_FILENAME__'


class YarcadiaPlugin(
    coverage.plugin.CoveragePlugin,
    coverage.plugin.FileTracer
):

    def __init__(self, options):
        self.config = coverage.config.CoverageConfig()
        self.config.from_args(**options)

        dirs = options.get("pylib_paths", "").split("\n")
        dirs = [d for d in dirs if d and not d.startswith("#")]
        self.pylib_paths = dirs

        self._filename = None
        self._exclude = None

        self._setup_file_filter()

    def _setup_file_filter(self):
        prefix_filter = os.environ.get('PYTHON_COVERAGE_PREFIX_FILTER', '')
        exclude_regexp = os.environ.get('PYTHON_COVERAGE_EXCLUDE_REGEXP', '')
        self.file_filter = coverage_utils.make_filter(prefix_filter, exclude_regexp)

    def configure(self, config):
        self._exclude = coverage.misc.join_regex(config.get_option('report:exclude_lines'))

    def get_pylib_paths(self):
        return self.pylib_paths

    def file_tracer(self, filename):
        if not filename.endswith(test_const.COVERAGE_PYTHON_EXTS):
            # Catch all generated modules (__file__ without proper extension)
            self._filename = SKIP_FILENAME
            return self

        if not self.file_filter(filename):
            # we need to catch all filtered out files (including cython) to pass them to get_source
            self._filename = SKIP_FILENAME
            return self

        if filename.endswith(".py"):
            self._filename = filename
            return self

        # Let cython plugin register it's own file tracer for pyx/pxi files
        return None

    def has_dynamic_source_filename(self):
        return False

    def source_filename(self):
        return self._filename

    def file_reporter(self, morf):
        source_root = os.environ.get("PYTHON_COVERAGE_ARCADIA_SOURCE_ROOT")
        if source_root:
            return FileReporter(morf, source_root, self, self._exclude)
        # use default file reporter
        return "python"


class FileReporter(coverage.python.PythonFileReporter):

    def __init__(self, morf, source_root, coverage=None, exclude=None):
        super(FileReporter, self).__init__(morf, coverage)
        self._source = get_source(morf, source_root)
        # use custom parser to provide proper way to get required source
        self._parser = Parser(morf, self._source, exclude)
        self._parser.parse_source()


class Parser(coverage.parser.PythonParser):

    def __init__(self, morf, source_code, exclude):
        # provide source code to avoid default way to get it
        super(Parser, self).__init__(text=source_code, filename=morf, exclude=exclude)


def get_source(filename, source_root):
    assert source_root

    if filename == SKIP_FILENAME:
        return ''

    abs_filename = os.path.join(source_root, filename)
    if not os.path.isfile(abs_filename):
        # it's fake generated package
        return u''

    return coverage.python.get_python_source(abs_filename, force_fs=True)


def coverage_init(reg, options):
    plugin = YarcadiaPlugin(options)
    reg.add_configurer(plugin)
    reg.add_file_tracer(plugin)

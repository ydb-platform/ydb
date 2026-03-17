import contextlib
import math
import os
import select
import statistics
import sys
import sysconfig
import time
from shlex import quote as shell_quote   # noqa
from shutil import which

# Currently there is a packaging issue for PEP-703,
# Until then psutil is disabled as a workaround.
# See: https://github.com/python/cpython/issues/116024
USE_PSUTIL = not bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
MS_WINDOWS = (sys.platform == 'win32')
MAC_OS = (sys.platform == 'darwin')
BSD = ('bsd' in sys.platform)

if MS_WINDOWS:
    import msvcrt

# A table of 95% confidence intervals for a two-tailed t distribution, as a
# function of the degrees of freedom. For larger degrees of freedom, we
# approximate. While this may look less elegant than simply calculating the
# critical value, those calculations suck. Look at
# http://www.math.unb.ca/~knight/utility/t-table.htm if you need more values.
_T_DIST_95_CONF_LEVELS = [0, 12.706, 4.303, 3.182, 2.776,
                          2.571, 2.447, 2.365, 2.306, 2.262,
                          2.228, 2.201, 2.179, 2.160, 2.145,
                          2.131, 2.120, 2.110, 2.101, 2.093,
                          2.086, 2.080, 2.074, 2.069, 2.064,
                          2.060, 2.056, 2.052, 2.048, 2.045,
                          2.042]


def tdist95conf_level(df):
    """Approximate the 95% confidence interval for Student's T distribution.

    Given the degrees of freedom, returns an approximation to the 95%
    confidence interval for the Student's T distribution.

    Args:
        df: An integer, the number of degrees of freedom.

    Returns:
        A float.
    """
    df = int(round(df))
    highest_table_df = len(_T_DIST_95_CONF_LEVELS)
    if df >= 200:
        return 1.960
    if df >= 100:
        return 1.984
    if df >= 80:
        return 1.990
    if df >= 60:
        return 2.000
    if df >= 50:
        return 2.009
    if df >= 40:
        return 2.021
    if df >= highest_table_df:
        return _T_DIST_95_CONF_LEVELS[highest_table_df - 1]
    return _T_DIST_95_CONF_LEVELS[df]


def pooled_sample_variance(sample1, sample2):
    """Find the pooled sample variance for two samples.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        Pooled sample variance, as a float.
    """
    deg_freedom = len(sample1) + len(sample2) - 2
    mean1 = statistics.mean(sample1)
    squares1 = ((x - mean1) ** 2 for x in sample1)
    mean2 = statistics.mean(sample2)
    squares2 = ((x - mean2) ** 2 for x in sample2)

    return (math.fsum(squares1) + math.fsum(squares2)) / float(deg_freedom)


def tscore(sample1, sample2):
    """Calculate a t-test score for the difference between two samples.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        The t-test score, as a float.
    """
    if len(sample1) != len(sample2):
        raise ValueError("different number of values")
    error = pooled_sample_variance(sample1, sample2) / len(sample1)
    diff = statistics.mean(sample1) - statistics.mean(sample2)
    return diff / math.sqrt(error * 2)


def is_significant(sample1, sample2):
    """Determine whether two samples differ significantly.

    This uses a Student's two-sample, two-tailed t-test with alpha=0.95.

    Args:
        sample1: one sample.
        sample2: the other sample.

    Returns:
        (significant, t_score) where significant is a bool indicating whether
        the two samples differ significantly; t_score is the score from the
        two-sample T test.
    """
    deg_freedom = len(sample1) + len(sample2) - 2
    critical_value = tdist95conf_level(deg_freedom)
    t_score = tscore(sample1, sample2)
    return (abs(t_score) >= critical_value, t_score)


def parse_run_list(run_list):
    run_list = run_list.strip()

    runs = []
    for part in run_list.split(','):
        part = part.strip()
        try:
            if '-' in part:
                parts = part.split('-', 1)
                first = int(parts[0])
                last = int(parts[1])
                for run in range(first, last + 1):
                    runs.append(run)
            else:
                runs.append(int(part))
        except ValueError:
            raise ValueError("invalid list of runs")

    if not runs:
        raise ValueError("empty list of runs")

    if min(runs) < 1:
        raise ValueError("number of runs starts at 1")

    return [run - 1 for run in runs]


def open_text(path, write=False):
    mode = "w" if write else "r"
    return open(path, mode, encoding="utf-8")


def read_first_line(path, error=False):
    try:
        with open_text(path) as fp:
            line = fp.readline()
        return line.rstrip()
    except OSError:
        if error:
            raise
        else:
            return ''


def proc_path(path):
    return os.path.join("/proc", path)


def sysfs_path(path):
    return os.path.join("/sys", path)


def python_implementation():
    return sys.implementation.name.lower()


def python_has_jit():
    implementation_name = python_implementation()
    if implementation_name == 'pypy':
        return sys.pypy_translation_info["translation.jit"]
    elif implementation_name in ['graalpython', 'graalpy']:
        return True
    elif implementation_name == 'cpython':
        jit_module = getattr(sys, '_jit', None)
        if jit_module is not None:
            return jit_module.is_enabled()
        return False
    elif hasattr(sys, "pyston_version_info") or "pyston_lite" in sys.modules:
        return True
    return False


@contextlib.contextmanager
def popen_killer(proc):
    try:
        yield
    except:   # noqa: E722
        # Close pipes
        if proc.stdin:
            proc.stdin.close()
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()
        try:
            proc.kill()
        except OSError:
            # process already terminated
            pass
        proc.wait()
        raise


def popen_communicate(proc):
    with popen_killer(proc):
        return proc.communicate()


def get_python_names(python1, python2):
    # FIXME: merge with format_filename_func() of __main__.py
    name1 = os.path.basename(python1)
    name2 = os.path.basename(python2)
    if name1 != name2:
        return (name1, name2)

    return (python1, python2)


def abs_executable(python):
    orig_python = python

    # Replace "~" with the user home directory
    python = os.path.expanduser(python)

    if os.path.dirname(python):
        # Get the absolute path to the directory of the program.
        #
        # Don't try to get the absolute path to the program, because symlink
        # must not be followed. The venv module of Python can use a symlink for
        # the "python" executable of the virtual environment. Running the
        # symlink adds the venv to sys.path, whereas running the real program
        # doesn't.
        path, python = os.path.split(python)
        path = os.path.realpath(path)
        python = os.path.join(path, python)
    else:
        python = which(python)
    if not python:
        print("ERROR: Unable to locate the Python executable: %r" % orig_python)
        sys.exit(1)

    return os.path.normpath(python)


def create_environ(inherit_environ, locale, copy_all):
    if copy_all:
        return os.environ
    env = {}
    copy_env = ["PATH", "HOME", "TEMP", "COMSPEC", "SystemRoot", "SystemDrive",
                # Python specific variables
                "PYTHONPATH", "PYTHON_CPU_COUNT", "PYTHON_GIL",
                # Pyperf specific variables
                "PYPERF_PERF_RECORD_DATA_DIR", "PYPERF_PERF_RECORD_EXTRA_OPTS",
                ]
    if locale:
        copy_env.extend(('LANG', 'LC_ADDRESS', 'LC_ALL', 'LC_COLLATE',
                         'LC_CTYPE', 'LC_IDENTIFICATION', 'LC_MEASUREMENT',
                         'LC_MESSAGES', 'LC_MONETARY', 'LC_NAME', 'LC_NUMERIC',
                         'LC_PAPER', 'LC_TELEPHONE', 'LC_TIME'))
    if inherit_environ:
        copy_env.extend(inherit_environ)

    for name in copy_env:
        if name in os.environ:
            env[name] = os.environ[name]
    return env


class _Pipe:
    _OPEN_MODE = "r"

    def __init__(self, fd):
        self._fd = fd
        self._file = None
        if MS_WINDOWS:
            self._handle = msvcrt.get_osfhandle(fd)

    @property
    def fd(self):
        return self._fd

    def close(self):
        fd = self._fd
        self._fd = None
        file = self._file
        self._file = None

        if file is not None:
            file.close()
        elif fd is not None:
            os.close(fd)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ReadPipe(_Pipe):
    def open_text(self):
        file = open(self._fd, "r", encoding="utf8")
        self._file = file
        return file

    def read_text(self, timeout=None):
        if timeout is not None:
            return self._read_text_timeout(timeout)
        else:
            with self.open_text() as rfile:
                return rfile.read()

    def _read_text_timeout(self, timeout):
        fd = self.fd
        os.set_blocking(fd, False)

        start_time = time.monotonic()
        output = []
        while True:
            if time.monotonic() - start_time > timeout:
                raise TimeoutError(f"Timed out after {timeout} seconds")
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                continue
            try:
                data = os.read(fd, 1024)
            except BlockingIOError:
                continue
            if not data:
                break
            output.append(data)

        data = b"".join(output)
        return data.decode("utf8")


class WritePipe(_Pipe):
    def to_subprocess(self):
        if MS_WINDOWS:
            os.set_handle_inheritable(self._handle, True)
            arg = self._handle
        else:
            os.set_inheritable(self._fd, True)
            arg = self._fd
        return str(arg)

    @classmethod
    def from_subprocess(cls, arg):
        arg = int(arg)
        if MS_WINDOWS:
            fd = msvcrt.open_osfhandle(arg, os.O_WRONLY)
        else:
            fd = arg
        return cls(fd)

    def open_text(self):
        file = open(self._fd, "w", encoding="utf8")
        self._file = file
        return file


def create_pipe():
    rfd, wfd = os.pipe()
    rpipe = ReadPipe(rfd)
    wpipe = WritePipe(wfd)
    return (rpipe, wpipe)


def median_abs_dev(values):
    # Median Absolute Deviation
    median = float(statistics.median(values))
    return statistics.median([abs(median - sample) for sample in values])


def percentile(values, p):
    if not isinstance(p, float) or not (0.0 <= p <= 1.0):
        raise ValueError("p must be a float in the range [0.0; 1.0]")

    values = sorted(values)
    if not values:
        raise ValueError("no value")

    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f != c:
        d0 = values[f] * (c - k)
        d1 = values[c] * (k - f)
        return d0 + d1
    else:
        return values[int(k)]


if hasattr(statistics, 'geometric_mean'):
    _geometric_mean = statistics.geometric_mean
else:
    def _geometric_mean(data):
        # Compute exp(fmean(map(log, data))) using floats
        data = list(map(math.log, data))

        # fmean(data)
        fmean = math.fsum(data) / len(data)

        return math.exp(fmean)


def geometric_mean(data):
    data = list(map(float, data))
    if not data:
        raise ValueError("empty data")
    return _geometric_mean(data)


def merge_profile_stats(profiler, dst):
    """
    Save pstats by merging into an existing file.
    """
    import pstats
    if os.path.isfile(dst):
        try:
            src_stats = pstats.Stats(profiler)
        except TypeError:
            # If no actual stats were collected, a TypeError is raised
            # and we don't need to merge anything into the output.
            return

        dst_stats = pstats.Stats(dst)
        dst_stats.add(src_stats)
        dst_stats.dump_stats(dst)
    else:
        profiler.dump_stats(dst)

import datetime
import errno
import math
import os.path
import sys

import statistics

from pyperf._metadata import (NUMBER_TYPES, parse_metadata,
                              _common_metadata, get_metadata_info,
                              _exclude_common_metadata)
from pyperf._formatter import DEFAULT_UNIT, format_values
from pyperf._utils import median_abs_dev, percentile


# JSON format history:
#
# '1.0' - (pyperf 1.0) warmup values are now per loop iteration,
#         rather than raw values.
# 6 - (pyperf 0.9.6) add common_metadata to the root: metadata common to all
#     benchmarks (common to all runs of all benchmarks); rename 'samples'
#     to 'values' in runs
# 5 - (pyperf 0.8.3) timestamps in metadata are now formatted using a space
#      separator
# 4 - (pyperf 0.7.4) warmups are now a lists of (loops, raw_sample)
#     rather than lists of samples
# 3 - (pyperf 0.7) add Run class
# 2 - (pyperf 0.6) support multiple benchmarks per file
# 1 - first version
_JSON_VERSION = '1.0'
_JSON_MAP_VERSION = {5: (0, 8, 3), 6: (0, 9, 6), '1.0': (1, 0)}

# Metadata checked by add_run(): all runs have must have the same
# value for these metadata (or no run must have this metadata)
_CHECKED_METADATA = (
    'aslr',
    'cpu_count',
    'cpu_model_name',
    'hostname',
    'inner_loops',
    'name',
    'platform',
    'python_executable',
    'python_implementation',
    'python_unicode',
    'python_version',
    'unit')


_UNSET = object()


def _check_warmups(warmups):
    for item in warmups:
        if not isinstance(item, tuple):
            return False
        if len(item) != 2:
            return False

        loops, value = item
        if not isinstance(loops, int):
            return False
        if loops < 1:
            return False

        if not isinstance(value, NUMBER_TYPES):
            return False
        if value < 0:
            return False

    return True


def _cached_attr(func):
    attr = '_' + func.__name__

    def method(self):
        value = getattr(self, attr)
        if value is not None:
            return value

        value = func(self)
        setattr(self, attr, value)
        return value

    return method


class Run:
    # Run is immutable, so it can be shared/exchanged between two benchmarks

    __slots__ = ('_warmups', '_values', '_metadata')

    def __init__(self, values, warmups=None,
                 metadata=None, collect_metadata=True):
        if any(not (isinstance(value, NUMBER_TYPES) and value > 0)
               for value in values):
            raise ValueError("values must be a sequence of number > 0.0")

        if warmups is not None and not _check_warmups(warmups):
            raise ValueError("warmups must be a sequence of (loops, value) "
                             "where loops is a int >= 1 and value "
                             "is a float >= 0.0")

        # tuple of (loops: int, value) items
        if warmups:
            self._warmups = tuple(warmups)
        else:
            self._warmups = None
        self._values = tuple(values)

        if not self._values and not self._warmups:
            raise ValueError("values and warmups are empty sequence")

        if collect_metadata:
            from pyperf._collect_metadata import collect_metadata as collect_func

            metadata2 = collect_func()
            if metadata is not None:
                metadata2.update(metadata)
                metadata = metadata2
            else:
                metadata = metadata2

        # Metadata dictionary
        if metadata:
            self._metadata = parse_metadata(metadata)
        else:
            self._metadata = {}

    def _replace(self, values=None, warmups=True, metadata=None):
        if values is None:
            values = self._values
        if warmups:
            warmups = self._warmups
        else:
            warmups = None
        if metadata is None:
            # share metadata dict since Run metadata is immutable
            metadata = self._metadata
        run = Run(values, warmups=warmups, collect_metadata=False)
        run._metadata = metadata
        return run

    def _is_calibration(self):
        # Run used to calibrate or recalibration the number of loops,
        # or to calibrate the number of warmups
        return (not self._values)

    def _is_calibration_loops(self):
        if not self._is_calibration():
            return False
        if self._has_metadata('calibrate_loops'):
            return True
        # backward compatibility with pyperf 1.1 and older
        return not any(self._has_metadata(name)
                       for name in ('recalibrate_loops', 'calibrate_warmups',
                                    'recalibrate_warmups'))

    def _is_recalibration_loops(self):
        return self._is_calibration() and self._has_metadata('recalibrate_loops')

    def _is_calibration_warmups(self):
        return self._is_calibration() and self._has_metadata('calibrate_warmups')

    def _is_recalibration_warmups(self):
        return self._is_calibration() and self._has_metadata('recalibrate_warmups')

    def _has_metadata(self, name):
        return (name in self._metadata)

    def _get_calibration_loops(self):
        metadata = self._metadata
        if 'calibrate_loops' in metadata:
            return metadata['calibrate_loops']
        if 'recalibrate_loops' in metadata:
            return metadata['recalibrate_loops']

        if self._is_calibration_loops():
            # backward compatibility with pyperf 1.1 and older
            return self.get_loops()

        raise ValueError("run is not a loop calibration")

    def _get_calibration_warmups(self):
        metadata = self._metadata
        if 'calibrate_warmups' in metadata:
            return metadata['calibrate_warmups']
        if 'recalibrate_warmups' in metadata:
            return metadata['recalibrate_warmups']
        raise ValueError("run is not a warmup calibration")

    def _get_name(self):
        return self._metadata.get('name', None)

    def get_metadata(self):
        return dict(self._metadata)

    @property
    def warmups(self):
        if self._warmups:
            return self._warmups
        else:
            return ()

    @property
    def values(self):
        return self._values

    def get_loops(self):
        return self._metadata.get('loops', 1)

    def get_inner_loops(self):
        return self._metadata.get('inner_loops', 1)

    def get_total_loops(self):
        return self.get_loops() * self.get_inner_loops()

    def _get_raw_values(self, warmups=False):
        raw_values = []

        if warmups and self._warmups:
            inner_loops = self.get_inner_loops()
            raw_values.extend(value * (loops * inner_loops)
                              for loops, value in self._warmups)

        total_loops = self.get_total_loops()
        raw_values.extend(value * total_loops for value in self._values)
        return raw_values

    def _remove_warmups(self):
        if not self._warmups:
            return self

        return self._replace(warmups=False)

    def _get_duration(self):
        duration = self._metadata.get('duration', None)
        if duration is not None:
            return duration
        raw_values = self._get_raw_values(warmups=True)
        return math.fsum(raw_values)

    def _get_date(self):
        return self._metadata.get('date', None)

    def _as_json(self, common_metadata):
        data = {}
        if self._warmups:
            data['warmups'] = self._warmups
        if self._values:
            data['values'] = self._values

        metadata = _exclude_common_metadata(self._metadata, common_metadata)
        if metadata:
            data['metadata'] = metadata
        return data

    @classmethod
    def _json_load(cls, version, run_data, common_metadata):
        metadata = run_data.get('metadata', {})
        if common_metadata:
            metadata = dict(common_metadata, **metadata)

        warmups = run_data.get('warmups', None)
        if warmups:
            if version >= (1, 0):
                warmups = [tuple(item) for item in warmups]
            else:
                inner_loops = metadata.get('inner_loops', 1)
                warmups = [(loops, raw_value / (loops * inner_loops))
                           for loops, raw_value in warmups]
        if version >= (0, 9, 6):
            values = run_data.get('values', ())
        else:
            values = run_data['samples']

        return cls(values,
                   warmups=warmups,
                   metadata=metadata,
                   collect_metadata=False)

    def _extract_metadata(self, name):
        value = self._metadata.get(name, None)
        if value is None:
            raise KeyError("run has no metadata %r" % name)

        info = get_metadata_info(name)
        if info.unit:
            metadata = dict(self._metadata, unit=info.unit)
        else:
            metadata = None

        if not isinstance(value, NUMBER_TYPES):
            raise TypeError("metadata %r value is not an integer: got %s"
                            % (name, type(value).__name__))

        return self._replace(values=(value,), warmups=False, metadata=metadata)

    def _remove_all_metadata(self):
        name = self._metadata.get('name', None)
        unit = self._metadata.get('unit', None)
        metadata = {}
        if name:
            metadata['name'] = name
        if unit:
            metadata['unit'] = unit
        return self._replace(metadata=metadata)

    def _update_metadata(self, metadata):
        if 'inner_loops' in metadata:
            inner_loops = self._metadata.get('inner_loops', None)
            if (inner_loops is not None
               and metadata['inner_loops'] != inner_loops):
                raise ValueError("inner_loops metadata cannot be modified")

        metadata2 = dict(self._metadata)
        metadata2.update(metadata)
        return self._replace(metadata=metadata2)


class Benchmark:
    def __init__(self, runs):
        self._runs = []   # list of Run objects
        self._clear_runs_cache()

        if not runs:
            raise ValueError("runs must be a non-empty sequence of Run objects")

        # A benchmark must have a name
        if not runs[0]._has_metadata('name'):
            raise ValueError("A benchmark must have a name: "
                             "the first run has no name metadata")

        for run in runs:
            self.add_run(run)

    def __repr__(self):
        return ('<Benchmark %r with %s runs>'
                % (self.get_name(), len(self._runs)))

    def get_name(self):
        run = self._runs[0]
        return run._get_name()

    def _get_common_metadata(self):
        if self._common_metadata is None:
            runs_metadata = [run._metadata for run in self._runs]
            self._common_metadata = _common_metadata(runs_metadata)
        return self._common_metadata

    def get_metadata(self):
        return dict(self._get_common_metadata())

    def get_total_duration(self):
        durations = [run._get_duration() for run in self._runs]
        return math.fsum(durations)

    def _get_run_property(self, get_property):
        # ignore calibration runs
        values = [get_property(run) for run in self._runs
                  if not run._is_calibration()]
        if len(set(values)) == 1:
            return values[0]

        # Compute the mean (float)
        return math.fsum(values) / len(values)

    def _get_nwarmup(self):
        return self._get_run_property(lambda run: len(run.warmups))

    def _get_nvalue_per_run(self):
        return self._get_run_property(lambda run: len(run.values))

    def get_loops(self):
        return self._get_run_property(lambda run: run.get_loops())

    def get_inner_loops(self):
        return self._get_run_property(lambda run: run.get_inner_loops())

    def get_total_loops(self):
        return self._get_run_property(lambda run: run.get_total_loops())

    def _clear_runs_cache(self, keep_common_metadata=False):
        self._values = None
        self._mean = None
        self._stdev = None
        self._median = None
        self._median_abs_dev = None
        if not keep_common_metadata:
            self._common_metadata = None
        self._dates = _UNSET

    @_cached_attr
    def mean(self):
        value = statistics.mean(self.get_values())
        # add_run() ensures that all values are greater than zero
        if value <= 0:
            raise ValueError("mean must be > 0")
        return value

    @_cached_attr
    def stdev(self):
        values = self.get_values()
        value = statistics.stdev(values)
        # add_run() ensures that all values are greater than zero
        if value < 0:
            raise ValueError("std dev must be >= 0")
        return value

    @_cached_attr
    def median(self):
        value = statistics.median(self.get_values())
        # add_run() ensures that all values are greater than zero
        if value <= 0:
            raise ValueError("median must be > 0")
        return value

    @_cached_attr
    def median_abs_dev(self):
        value = median_abs_dev(self.get_values())
        # add_run() ensures that all values are greater than zero
        if value < 0:
            raise ValueError("MAD must be >= 0")
        return value

    def required_nprocesses(self):
        """
        Determines the number of separate process runs that would be required
        achieve stable results. Specifically, the target is to have 95%
        certainty that there is a variance of less than 1%. If the result is
        greater than the number of processes recorded in the input data, the
        value is meaningless and only means "more samples are required".

        The method used is described in this Wikipedia article about estimating
        the sampling of a mean:

        https://en.wikipedia.org/wiki/Sample_size_determination#Estimation_of_a_mean
        """
        # Get the means of the values per process. The values within the process
        # often vary considerably (e.g. due to cache effects), but the variances
        # between processes should be fairly consistent. Additionally, this
        # value is intended to be advice for the number of processes to run.
        values = []
        for run in self._runs:
            if len(run.values):
                values.append(statistics.mean(run.values))

        if len(values) < 2:
            return None

        total = math.fsum(values)
        mean = total / len(values)
        stddev = statistics.stdev(values)

        # Normalize the stddev so we can target "percentage changed" rather than
        # absolute time
        sigma = stddev / mean

        # 95% certainty
        Z = 1.96
        # 1% variation
        W = 0.01

        # (4Z²σ²)/(W²)
        return math.ceil((4 * Z ** 2 * sigma ** 2) / (W ** 2))

    def percentile(self, p):
        if not (0 <= p <= 100):
            raise ValueError("p must be in the range [0; 100]")
        return percentile(self.get_values(), p / 100.0)

    def add_run(self, run):
        if not isinstance(run, Run):
            raise TypeError("Run expected, got %s" % type(run).__name__)

        # Don't check metadata for the first run
        if self._runs:
            metadata = self._get_common_metadata()
            run_metata = run._metadata
            for key in _CHECKED_METADATA:
                value = metadata.get(key, None)
                run_value = run_metata.get(key, None)
                if run_value != value:
                    raise ValueError("incompatible benchmark, metadata %s is "
                                     "different: current=%s, run=%s"
                                     % (key, value, run_value))

        if self._common_metadata is not None:
            # Update common metadata
            for name, value in list(self._common_metadata.items()):
                if run._metadata.get(name, None) != value:
                    del self._common_metadata[name]
        self._clear_runs_cache(keep_common_metadata=True)

        self._runs.append(run)

    def get_unit(self):
        run = self._runs[0]
        return run._metadata.get('unit', DEFAULT_UNIT)

    def format_values(self, values):
        unit = self.get_unit()
        return format_values(unit, values)

    def format_value(self, value):
        return self.format_values((value,))[0]

    def get_nrun(self):
        return len(self._runs)

    def get_runs(self):
        return list(self._runs)

    def get_nvalue(self):
        if self._values is not None:
            return len(self._values)
        else:
            return sum(len(run.values) for run in self._runs)

    def get_values(self):
        if self._values is not None:
            return self._values

        values = []
        for run in self._runs:
            values.extend(run.values)
        values = tuple(values)
        self._values = values
        return values

    def _get_raw_values(self, warmups=False):
        raw_values = []
        for run in self._runs:
            raw_values.extend(run._get_raw_values(warmups))
        return raw_values

    def _only_calibration(self):
        return all(run._is_calibration() for run in self._runs)

    @classmethod
    def _json_load(cls, version, data, suite_metadata):
        if version >= (0, 9, 6):
            metadata = data.get('metadata', {})
        else:
            metadata = data.get('common_metadata', {})
        metadata = parse_metadata(metadata)
        if suite_metadata:
            metadata = dict(suite_metadata, **metadata)

        runs = []
        for run_data in data['runs']:
            run = Run._json_load(version, run_data, metadata)
            # Don't call add_run() to avoid O(n) complexity:
            # expect that runs were already validated before being written
            # into a JSON file
            runs.append(run)

        return cls(runs)

    def _as_json(self, suite_metadata):
        metadata = self._get_common_metadata()
        common_metadata = dict(metadata, **suite_metadata)

        data = {'runs': [run._as_json(common_metadata) for run in self._runs]}
        metadata = _exclude_common_metadata(metadata, suite_metadata)
        if metadata:
            data['metadata'] = metadata
        return data

    @staticmethod
    def load(file):
        suite = BenchmarkSuite.load(file)
        benchmarks = suite.get_benchmarks()
        if len(benchmarks) != 1:
            raise ValueError("expected 1 benchmark, got %s" % len(benchmarks))
        return benchmarks[0]

    @staticmethod
    def loads(string):
        suite = BenchmarkSuite.loads(string)
        benchmarks = suite.get_benchmarks()
        if len(benchmarks) != 1:
            raise ValueError("expected 1 benchmark, got %s" % len(benchmarks))
        return benchmarks[0]

    def dump(self, file, compact=True, replace=False):
        suite = BenchmarkSuite([self])
        suite.dump(file, compact=compact, replace=replace)

    def _replace_runs(self, new_runs):
        if not new_runs:
            raise ValueError("no more runs")
        self._runs[:] = new_runs
        self._clear_runs_cache()

    def _filter_runs(self, include, only_runs):
        if include:
            old_runs = self._runs
            max_index = len(old_runs) - 1
            runs = []
            for index in only_runs:
                if index <= max_index:
                    runs.append(old_runs[index])
        else:
            runs = self._runs
            max_index = len(runs) - 1
            for index in reversed(only_runs):
                if index <= max_index:
                    del runs[index]
        self._replace_runs(runs)

    def _remove_warmups(self):
        new_runs = [run._remove_warmups() for run in self._runs]
        self._replace_runs(new_runs)

    def add_runs(self, benchmark):
        if not isinstance(benchmark, Benchmark):
            raise TypeError("expected Benchmark, got %s"
                            % type(benchmark).__name__)

        if benchmark is self:
            raise ValueError("cannot add a benchmark to itself")

        for run in benchmark._runs:
            self.add_run(run)

    def get_dates(self):
        if self._dates is not _UNSET:
            return self._dates

        start = None
        end = None
        for run in self._runs:
            run_start = run._get_date()
            if run_start is None:
                continue
            run_start = datetime.datetime.fromisoformat(run_start)

            duration = run._get_duration()
            duration = int(math.ceil(duration))
            run_end = run_start + datetime.timedelta(seconds=duration)
            if start is None or run_start < start:
                start = run_start
            if end is None or run_end > end:
                end = run_end

        if start is not None:
            self._dates = (start, end)
        else:
            self._dates = None
        return self._dates

    def _extract_metadata(self, name):
        new_runs = [run._extract_metadata(name) for run in self._runs]
        self._replace_runs(new_runs)

    def _remove_all_metadata(self):
        new_runs = [run._remove_all_metadata() for run in self._runs]
        self._replace_runs(new_runs)

    def update_metadata(self, metadata):
        metadata = parse_metadata(metadata)
        if not metadata:
            return self

        new_runs = [run._update_metadata(metadata) for run in self._runs]
        self._replace_runs(new_runs)


class BenchmarkSuite:
    def __init__(self, benchmarks, filename=None):
        if not benchmarks:
            raise ValueError("benchmarks must be a non-empty "
                             "sequence of Benchmark objects")

        self.filename = filename
        self._benchmarks = []
        for benchmark in benchmarks:
            self.add_benchmark(benchmark)

    def get_benchmark_names(self):
        return [bench.get_name() for bench in self]

    def get_metadata(self):
        benchs_metadata = [bench._get_common_metadata()
                           for bench in self._benchmarks]
        return _common_metadata(benchs_metadata)

    def __len__(self):
        return len(self._benchmarks)

    def __iter__(self):
        return iter(self._benchmarks)

    def _add_benchmark_runs(self, benchmark):
        name = benchmark.get_name()
        try:
            existing = self.get_benchmark(name)
        except KeyError:
            self.add_benchmark(benchmark)
        else:
            existing.add_runs(benchmark)

    def add_runs(self, result):
        if isinstance(result, Benchmark):
            self._add_benchmark_runs(result)
        elif isinstance(result, BenchmarkSuite):
            for benchmark in result:
                self._add_benchmark_runs(benchmark)
        else:
            raise TypeError("expect Benchmark or BenchmarkSuite, got %s"
                            % type(result).__name__)

    def get_benchmark(self, name):
        for bench in self._benchmarks:
            if bench.get_name() == name:
                return bench
        raise KeyError("there is no benchmark called %r" % name)

    def get_benchmarks(self):
        return list(self._benchmarks)

    def add_benchmark(self, benchmark):
        if benchmark in self._benchmarks:
            raise ValueError("benchmark already part of the suite")

        name = benchmark.get_name()
        if name:
            try:
                self.get_benchmark(name)
            except KeyError:
                pass
            else:
                raise ValueError("the suite has already a benchmark called %r"
                                 % name)

        self._benchmarks.append(benchmark)

    @classmethod
    def _json_load(cls, filename, data):
        version = data.get('version')
        version_info = _JSON_MAP_VERSION.get(version)
        if not version_info:
            raise ValueError("file format version %r not supported" % version)
        benchmarks_json = data['benchmarks']

        if version_info >= (0, 9, 6):
            metadata = data.get('metadata', {})
            if metadata is not None:
                metadata = parse_metadata(metadata)
        else:
            metadata = {}

        benchmarks = []
        for bench_data in benchmarks_json:
            benchmark = Benchmark._json_load(version_info, bench_data, metadata)
            benchmarks.append(benchmark)
        suite = cls(benchmarks, filename=filename)

        if not suite:
            raise ValueError("the file doesn't contain any benchmark")

        return suite

    @staticmethod
    def _load_open(filename):
        if isinstance(filename, bytes):
            suffix = b'.gz'
        else:
            suffix = '.gz'

        if filename.endswith(suffix):
            # Use lazy import to limit imports on 'import pyperf'
            import gzip
            return gzip.open(filename, "rt", encoding="utf-8")
        else:
            return open(filename, "r", encoding="utf-8")

    @classmethod
    def load(cls, file):
        # Use lazy import to limit imports on 'import pyperf'
        import json

        if isinstance(file, (bytes, str)):
            if file != '-':
                filename = file
                fp = cls._load_open(filename)
                with fp:
                    data = json.load(fp)
            else:
                filename = '<stdin>'
                data = json.load(sys.stdin)
        else:
            # file is a file object
            filename = getattr(file, 'name', None)
            data = json.load(file)

        return cls._json_load(filename, data)

    @classmethod
    def loads(cls, string):
        # Use lazy import to limit imports on 'import pyperf'
        import json

        data = json.loads(string)
        return cls._json_load(None, data)

    @staticmethod
    def _dump_open(filename, replace):
        if isinstance(filename, bytes):
            suffix = b'.gz'
        else:
            suffix = '.gz'

        if not replace and os.path.exists(filename):
            raise OSError(errno.EEXIST, "File already exists")

        if filename.endswith(suffix):
            # Use lazy import to limit imports on 'import pyperf'
            import gzip

            return gzip.open(filename, mode="wt", encoding="utf-8")
        else:
            return open(filename, "w", encoding="utf-8")

    def _as_json(self):
        metadata = self.get_metadata()
        benchmarks = [benchmark._as_json(metadata)
                      for benchmark in self._benchmarks]
        data = {'version': _JSON_VERSION, 'benchmarks': benchmarks}
        if metadata:
            data['metadata'] = metadata
        return data

    def dump(self, file, compact=True, replace=False):
        # Use lazy import to limit imports on 'import pyperf'
        import json

        data = self._as_json()

        def dump(data, fp, compact):
            kw = {}
            if compact:
                kw['separators'] = (',', ':')
            else:
                kw['indent'] = 4
            json.dump(data, fp, sort_keys=True, **kw)
            fp.write("\n")
            fp.flush()

        if isinstance(file, (bytes, str)):
            fp = self._dump_open(file, replace)
            with fp:
                dump(data, fp, compact)
                fp.close()
        else:
            # file is a file object
            dump(data, file, compact)

    def _replace_benchmarks(self, benchmarks):
        if not benchmarks:
            raise ValueError("empty benchmark suite")
        self._benchmarks[:] = benchmarks

    def _convert_include_benchmark(self, names):
        name_set = set(names)
        benchmarks = [bench for bench in self
                      if bench.get_name() in name_set]
        if not benchmarks:
            raise KeyError("no benchmark found with name in %r" % names)
        self._replace_benchmarks(benchmarks)

    def _convert_exclude_benchmark(self, names):
        name_set = set(names)
        benchmarks = [bench for bench in self
                      if bench.get_name() not in name_set]
        self._replace_benchmarks(benchmarks)

    def get_total_duration(self):
        durations = [benchmark.get_total_duration() for benchmark in self]
        return math.fsum(durations)

    def get_dates(self):
        start = None
        end = None
        for benchmark in self:
            dates = benchmark.get_dates()
            if not dates:
                continue
            if start is None or dates[0] < start:
                start = dates[0]
            if end is None or dates[1] > end:
                end = dates[1]
        if start is not None:
            return (start, end)
        else:
            return None


def add_runs(filename, result):
    if os.path.exists(filename):
        suite = BenchmarkSuite.load(filename)
        suite.add_runs(result)
        suite.dump(filename, replace=True)
    else:
        result.dump(filename)


def _load_suite_from_pipe(bench_json):
    lines = bench_json.split("\n")
    result = None
    for line in lines:
        if not line:
            continue
        suite = BenchmarkSuite.loads(line)
        if result is not None:
            for bench in suite:
                result.add_benchmark(bench)
        else:
            result = suite
    return result

def test_import():
    import line_profiler
    assert hasattr(line_profiler, 'LineProfiler')
    assert hasattr(line_profiler, '__version__')


def test_version():
    import line_profiler
    from packaging.version import Version
    import kernprof
    line_profiler_version1 = Version(line_profiler.__version__)
    line_profiler_version2 = Version(line_profiler.line_profiler.__version__)
    kernprof_version = Version(kernprof.__version__)
    assert line_profiler_version1 == line_profiler_version2 == kernprof_version, (
        'All 3 places should have the same version')

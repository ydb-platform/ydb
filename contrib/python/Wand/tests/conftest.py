import os
from pathlib import Path

from pytest import fixture, mark

from wand.version import MAGICK_VERSION, VERSION


def pytest_addoption(parser):
    parser.addoption('--skip-slow', action='store_true',
                     help='Skip slow tests')
    parser.addoption('--skip-pdf', action='store_true',
                     help='Skip any test with PDF documents.')
    parser.addoption('--skip-fft', action='store_true',
                     help='Skip any test with Forward Fourier Transform.')
    parser.addoption('--no-pdf', action='store_true',
                     help='Alias to --skip-pdf.')


def pytest_collection_modifyitems(config, items):
    skip_slow = False
    skip_pdf = False
    skip_fft = False
    if config.getoption('--skip-slow'):
        skip_slow = mark.skip('skipped; --skip-slow option was used')
    if config.getoption('--skip-pdf'):
        skip_pdf = mark.skip('skipped; --skip-pdf option was used')
    if config.getoption('--skip-fft'):
        skip_fft = mark.skip('skipped; --skip-fft option was used')
    if config.getoption('--no-pdf'):
        skip_pdf = mark.skip('skipped; --skip-pdf option was used')
    for item in items:
        if skip_slow and 'slow' in item.keywords:
            item.add_marker(skip_slow)
        if skip_pdf and 'pdf' in item.keywords:
            item.add_marker(skip_pdf)
        if skip_fft and 'fft' in item.keywords:
            item.add_marker(skip_fft)


def pytest_configure(config):
    config.addinivalue_line(
        'markers', 'slow: marks test as slow-running'
    )
    config.addinivalue_line(
        'markers', 'pdf: marks test as PDF/Ghostscript dependent'
    )
    config.addinivalue_line(
        'markers', 'fft: marks test as Forward Fourier Transform dependent'
    )


def pytest_report_header(config):
    versions = (VERSION, os.linesep, MAGICK_VERSION)
    return "Wand Version: {0}{1}ImageMagick Version: {2}".format(*versions)


@fixture
def fx_asset():
    """The fixture that provides :class:`pathlib.Path` instance that
    points the :file:`assets` directory.  You can use this in test
    functions::

        def test_something(fx_asset):
            monalisa = str(fx_asset.joinpath('mona-lisa.jpg'))
            with open(monalisa) as f:
                assert f.tell() == 0

    .. versionchanged:: 0.6.11
       Switch `py.path.local` to `pathlib.Path`.
    """
    import yatest.common as yc
    return Path(yc.source_path('contrib/python/Wand/tests/conftest.py')).with_name('assets')

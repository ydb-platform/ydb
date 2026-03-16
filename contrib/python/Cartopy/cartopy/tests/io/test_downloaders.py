# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

import contextlib
from pathlib import Path
from unittest import mock

import pytest

import cartopy
import cartopy.io as cio
from cartopy.io.shapereader import NEShpDownloader


def test_Downloader_data():
    di = cio.Downloader('https://testing.com/{category}/{name}.zip',
                        str(Path('{data_dir}') / '{category}' / 'shape.shp'),
                        str(Path('/project') / 'foobar' / '{category}' /
                            'sample.shp'))

    replacement_dict = {'category': 'example',
                        'name': 'test',
                        'data_dir': str(Path('/wibble') / 'foo' / 'bar')}

    assert di.url(replacement_dict) == 'https://testing.com/example/test.zip'

    assert (di.target_path(replacement_dict) ==
            Path('/wibble') / 'foo' / 'bar' / 'example' / 'shape.shp')

    assert (di.pre_downloaded_path(replacement_dict) ==
            Path('/project/foobar/example/sample.shp'))


@contextlib.contextmanager
def config_replace(replacement_dict):
    """
    Provides a context manager to replace the ``cartopy.config['downloaders']``
    dict with the given dictionary. Great for testing purposes!

    """
    with contextlib.ExitStack() as stack:
        stack.callback(cartopy.config.__setitem__, 'downloaders',
                       cartopy.config['downloaders'])
        cartopy.config['downloaders'] = replacement_dict

        yield


@pytest.fixture
def download_to_temp(tmp_path_factory):
    """
    Context manager which defaults the "data_dir" to a temporary directory
    which is automatically cleaned up on exit.

    """
    with contextlib.ExitStack() as stack:
        stack.callback(cartopy.config.__setitem__, 'downloaders',
                       cartopy.config['downloaders'].copy())
        stack.callback(cartopy.config.__setitem__, 'data_dir',
                       cartopy.config['data_dir'])

        tmp_dir = tmp_path_factory.mktemp('cartopy_data')
        cartopy.config['data_dir'] = str(tmp_dir)

        yield tmp_dir


def test_from_config():
    generic_url = 'https://example.com/generic_ne/{name}.zip'

    land_downloader = cio.Downloader(generic_url, '', '')
    generic_ne_downloader = cio.Downloader(generic_url, '', '')

    ocean_spec = ('shapefile', 'natural_earth', '110m', 'physical', 'ocean')
    land_spec = ('shapefile', 'natural_earth', '110m', 'physical', 'land')
    generic_spec = ('shapefile', 'natural_earth')

    target_config = {land_spec: land_downloader,
                     generic_spec: generic_ne_downloader,
                     }

    with config_replace(target_config):
        # ocean spec is not explicitly in the config, but a subset of it is,
        # so check that an appropriate downloader is returned
        r = cio.Downloader.from_config(ocean_spec)

        # check the resulting download item produces a sensible url.
        assert (r.url({'name': 'ocean'}) ==
                'https://example.com/generic_ne/ocean.zip')

        r = cio.Downloader.from_config(land_spec)
        assert r is land_downloader


@pytest.mark.network
def test_downloading_simple_ascii(download_to_temp):
    # downloads a file from the Google APIs. (very high uptime and file will
    # always be there - if this goes down, most of the internet would break!)
    # to test the downloading mechanisms.
    file_url = 'https://ajax.googleapis.com/ajax/libs/jquery/1.8.2/{name}.js'

    format_dict = {'name': 'jquery'}

    target_template = str(download_to_temp / '{name}.txt')
    tmp_fname = Path(target_template.format(**format_dict))

    dnld_item = cio.Downloader(file_url, target_template)

    assert dnld_item.target_path(format_dict) == tmp_fname

    with pytest.warns(cio.DownloadWarning):
        assert dnld_item.path(format_dict) == tmp_fname

    with open(tmp_fname) as fh:
        fh.readline()
        assert fh.readline() == " * jQuery JavaScript Library v1.8.2\n"

    # check that calling path again doesn't try re-downloading
    with mock.patch.object(dnld_item, 'acquire_resource',
                           wraps=dnld_item.acquire_resource) as counter:
        assert dnld_item.path(format_dict) == tmp_fname
    counter.assert_not_called()


@pytest.mark.network
def test_natural_earth_downloader(tmp_path):
    # downloads a file to a temporary location, and uses that temporary
    # location, then:
    #   * Checks that the file is only downloaded once even when calling
    #     multiple times
    #   * Checks that shapefiles have all the necessary files when downloaded
    #   * Checks that providing a path in a download item gets used rather
    #     than triggering another download
    shp_path_template = str(tmp_path / '{category}_{resolution}_{name}.shp')

    # picking a small-ish file to speed up download times, the file itself
    # isn't important - it is the download mechanism that is.
    format_dict = {'category': 'physical',
                   'name': 'rivers_lake_centerlines',
                   'resolution': '110m'}

    dnld_item = NEShpDownloader(target_path_template=shp_path_template)

    # check that the file gets downloaded the first time path is called
    with mock.patch.object(dnld_item, 'acquire_resource',
                           wraps=dnld_item.acquire_resource) as counter:
        with pytest.warns(cartopy.io.DownloadWarning, match="Downloading:"):
            shp_path = dnld_item.path(format_dict)
    counter.assert_called_once()

    assert shp_path_template.format(**format_dict) == str(shp_path)

    # check that calling path again doesn't try re-downloading
    with mock.patch.object(dnld_item, 'acquire_resource',
                           wraps=dnld_item.acquire_resource) as counter:
        assert dnld_item.path(format_dict) == shp_path
    counter.assert_not_called()

    # check that we have the shp and the shx
    exts = ['.shp', '.shx']
    for ext in exts:
        fname = shp_path.with_suffix(ext)
        assert fname.exists(), \
            f"Shapefile's {ext} file doesn't exist in {fname}"

    # check that providing a pre downloaded path actually works
    pre_dnld = NEShpDownloader(target_path_template='/not/a/real/file.txt',
                               pre_downloaded_path_template=str(shp_path))

    # check that the pre_dnld downloader doesn't re-download, but instead
    # uses the path of the previously downloaded item
    with mock.patch.object(pre_dnld, 'acquire_resource',
                           wraps=pre_dnld.acquire_resource) as counter:
        assert pre_dnld.path(format_dict) == shp_path
    counter.assert_not_called()

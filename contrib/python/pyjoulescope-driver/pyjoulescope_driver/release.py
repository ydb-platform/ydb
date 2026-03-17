# Copyright 2022 Jetperch LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Handle device programming for firmware and gateware.

For finer control on updating, use the jsdrv_util application.  For example:

jsdrv_util reset update1 && jsdrv_util mem_erase c/upd2 && jsdrv_util mem_write c/upd2 "C:/joulescope/js220/images/ctrl_release/updater2/js220_ctrl_updater2_1_0_1.img" && jsdrv_util reset update2
jsdrv_util reset update2 && jsdrv_util mem_erase c/upd1 && jsdrv_util mem_write c/upd1 "C:/joulescope/js220/images/ctrl_release/updater1/js220_ctrl_updater1_1_0_1.img" && jsdrv_util reset update1
jsdrv_util reset update1 && jsdrv_util mem_erase c/app && jsdrv_util mem_write c/app "C:/joulescope/js220/images/ctrl_release/app/js220_ctrl_app_1_0_1.img" && jsdrv_util reset app
jsdrv_util reset update1 && jsdrv_util mem_erase s/app1 && jsdrv_util mem_write s/app1 "C:/joulescope/js220/images/fpga_release/js220_fpga_1_0_1.img" && jsdrv_util reset app
"""

import json
import logging
import os
import struct
import sys
import requests
import pkgutil


_log = logging.getLogger(__name__)
APP = 'joulescope_driver'
MATURITY = ['alpha', 'beta', 'stable']
MY_PATH = os.path.dirname(os.path.abspath(__file__))
URL = 'https://download.joulescope.com/firmware/js220/'
URL_TIMEOUT = 30.0
MAGIC_HEADER = b'joulescope_img_section\x0D\x0A \x0A \x1A  \xB2\x1C'
SUBTYPE_CTRL_UPDATER1 = 1
SUBTYPE_CTRL_UPDATER2 = 2
SUBTYPE_CTRL_APP = 3
SUBTYPE_SENSOR_FPGA = 4
CTRL_TYPE_APP = 0
CTRL_TYPE_UPD1 = 1
CTRL_TYPE_UPD2 = 2


def _targets_generate():
    d = {}
    keys = ['subtype', 'name', 'mem_region', 'target_id']
    infos = [
        # [subtype,              name,           mem_ region,     target]
        [SUBTYPE_CTRL_UPDATER2, 'ctrl_updater2', 'h/mem/c/upd2', CTRL_TYPE_UPD2],
        [SUBTYPE_CTRL_UPDATER1, 'ctrl_updater1', 'h/mem/c/upd1', CTRL_TYPE_UPD1],
        [SUBTYPE_SENSOR_FPGA,   'sensor_fpga',   'h/mem/s/app1', None],
        [SUBTYPE_CTRL_APP,      'ctrl_app',      'h/mem/c/app',  CTRL_TYPE_APP],
    ]
    for info in infos:
        d[info[0]] = dict(zip(keys, info))
    return d


TARGETS = _targets_generate()


def url_save(url, filename):
    """Save a file from the distribution.

    :param url: The relative url from the main distribution URL.
    :param filename: The local filename.
    """
    _log.info(f'url_save({url}, {filename})')
    r = requests.get(URL + url, timeout=URL_TIMEOUT)
    if r.status_code != 200:
        raise FileNotFoundError(url)
    with open(filename, 'wb') as f:
        for chunk in r:
            f.write(chunk)


def release_path():
    """Get the local program storage path.

    :return: The path for storing local data.
        This path is guaranteed to exist on successful return.
    """
    if 'win32' in sys.platform:
        from win32com.shell import shell, shellcon
        path = shell.SHGetFolderPath(0, shellcon.CSIDL_LOCAL_APPDATA, None, 0)
        path = os.path.join(path, APP)
    else:
        path = os.path.expanduser('~')
        path = os.path.join(path, '.' + APP)
    path = os.path.join(path, 'program')
    os.makedirs(path, exist_ok=True)
    return path


def dist_path():
    """Get the path for distribution images.

    :return: The distribution image path.  This path is NOT
        guaranteed to exist.
    """
    return MY_PATH


def _load_file_from_network(path, force_download=None):
    cache_path = release_path()
    fname = os.path.join(cache_path, path)
    if force_download or not os.path.isfile(fname):
        os.makedirs(os.path.dirname(fname), exist_ok=True)
        url_save(path, fname)
    with open(fname, 'rb') as f:
        return f.read()


def _load_from_network(maturity=None, force_download=None):
    path = release_path()
    index_filename = os.path.join(path, 'index.json')
    url_save('index.json', index_filename)
    with open(index_filename, 'rt') as f:
        index_file = json.load(f)

    result = {}
    for target, target_value in index_file.items():
        result[target] = {}
        for m, v in target_value.items():
            if maturity is not None and m != maturity:
                continue
            v['img'] = _load_file_from_network(v['path'], force_download=force_download)
            v['changelog'] = _load_file_from_network(v['changelog'], force_download=force_download)
            result[target][m] = v
    return result


def releases_get_from_network(force_download=None, dist_save=None):
    """Get releases from the network.

    :param force_download: Force download, even if local files exist.
    :param dist_save: Save to local dist directory.
    """
    if dist_save:
        path = dist_path()
        os.makedirs(path, exist_ok=True)
    else:
        path = release_path()
    result = _load_from_network(force_download=force_download)
    for m in MATURITY:
        dst = os.path.join(path, f'img_{m}.img')
        with open(dst, 'wb') as f:
            for target_info in TARGETS.values():
                name = target_info['name']
                img = result[name][m]['img']
                subtype = struct.unpack('<H', img[296:298])[0]
                if target_info['subtype'] != subtype:
                    raise RuntimeError('subtype mismatch')
                f.write(img)
    return result


def release_get(maturity, force_download=None):
    """Get the available release.

    :param maturity: The desired maturity level, which is one of
        * alpha
        * beta
        * stable
    :param force_download: True to force download, even if local files
        are already available.  None (default) or False to use local
        files if present.
    :return: The release image as bytes
    :raise Exception: If could not load the release image.
    """
    src = None
    if maturity is None:
        maturity = 'stable'
    maturity = maturity.lower()
    if maturity not in MATURITY:
        raise ValueError(f'invalid maturity level {maturity} not in {MATURITY}')
    fname = f'img_{maturity}.img'
    rpath = os.path.join(release_path(), fname)
    if not force_download:
        try:
            img = pkgutil.get_data('pyjoulescope_driver', fname)
            _log.info('release_get found %s using pkgutil.get_data', fname)
            return img
        except FileNotFoundError:
            pass
        if os.path.isfile(rpath):
            _log.info('release_get found %s in release path', fname)
            src = rpath
    if src is None:
        try:
            releases_get_from_network(force_download=True)
            if os.path.isfile(rpath):
                _log.info('release_get downloaded %s from network', fname)
        except Exception:
            _log.warning('releases_get_from_network failed')
            raise
        src = rpath
    try:
        with open(src, 'rb') as f:
            return f.read()
    except Exception:
        _log.warning('release_get could not read %s', fname)
        raise


def release_to_segments(img: bytes):
    """Convert the binary release image into segments.

    :param img: The binary release image.
    :return: Map of segments.
    """
    segments = {}
    while len(img):
        if not img.startswith(MAGIC_HEADER):
            raise RuntimeError('invalid image')
        if len(img) < 1024:
            raise RuntimeError('invalid image')
        sz = struct.unpack('<I', img[32:36])[0]
        ver = struct.unpack('<I', img[288:292])[0]
        subtype = struct.unpack('<H', img[296:298])[0]
        segments[subtype] = {
            'subtype': subtype,
            'name': TARGETS[subtype]['name'],
            'version': ver,
            'img': img[:sz]
        }
        img = img[sz:]
    return segments


def release_to_available(img: bytes):
    """Convert the binary release image into metadata.

    :param img: The binary release image.
    :return: The available version dict[name, version]
        for each segment as a 32-bit integer
        with major8.minor8.patch16.
        The segment names are:
        * app
        * updater1
        * updater2
        * fpga
    """
    r = {}
    segments = release_to_segments(img)
    for value in segments.values():
        name = value['name'].split('_')[1]
        r[name] = value['version']
    return r


if __name__ == '__main__':
    releases_get_from_network(force_download=True, dist_save=True)

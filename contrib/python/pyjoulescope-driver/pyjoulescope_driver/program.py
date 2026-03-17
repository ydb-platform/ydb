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


from pyjoulescope_driver import Driver
from pyjoulescope_driver.release import release_to_segments, \
    SUBTYPE_CTRL_APP, SUBTYPE_CTRL_UPDATER2, \
    SUBTYPE_CTRL_UPDATER1, SUBTYPE_SENSOR_FPGA, \
    TARGETS
import logging
import time


_log = logging.getLogger(__name__)
SUBTYPE_RESET = {
    SUBTYPE_CTRL_UPDATER1: 'update1',
    SUBTYPE_CTRL_UPDATER2: 'update2',
    SUBTYPE_CTRL_APP:      'app',
    SUBTYPE_SENSOR_FPGA:   None,
}


def version_to_str(version):
    if version is None:
        return '?.?.?'
    if isinstance(version, str):
        return version
    v_patch = version & 0xffff
    v_minor = (version >> 16) & 0xff
    v_major = (version >> 24) & 0xff
    return f'{v_major}.{v_minor}.{v_patch}'


class Programmer:

    def __init__(self, driver, device_path):
        self._driver = driver
        self._path = device_path
        if device_path[2] != '&':
            self._running_subtype = SUBTYPE_CTRL_APP
            self._path_app = device_path
            self._path_updater = device_path[:2] + '&' + device_path[2:]
        else:
            self._running_subtype = None  # either updater
            self._path_app = device_path[:2] + device_path[3:]
            self._path_updater = device_path

    @property
    def is_in_app(self):
        return self._path == self._path_app

    @property
    def version(self):
        return self._driver.query(self._path + '/c/fw/version')

    @property
    def fpga_version(self):
        return self._driver.query(self._path + '/s/fpga/version')

    @property
    def target_id(self):
        return self._driver.query(self._path + '/c/target')

    def _device_await(self, device_path, timeout=None):
        timeout = 5.0 if timeout is None else float(timeout)
        t_end = time.time() + timeout

        while True:
            device_paths = self._driver.device_paths()
            devices = [d for d in device_paths if d == self._path]
            if len(devices):
                if time.time() > t_end:
                    msg = f'timeout waiting for {self._path} removal'
                    _log.warning(msg)
                    raise TimeoutError(msg)
            else:
                break

        while True:
            device_paths = self._driver.device_paths()
            devices = [d for d in device_paths if d == device_path]
            if len(devices):
                self._path = devices[0]
                return
            if time.time() > t_end:
                msg = f'timeout waiting for {device_path} in {device_paths}'
                _log.warning(msg)
                raise TimeoutError(msg)
            time.sleep(0.1)

    def reset_to(self, subtype):
        if self._running_subtype == subtype:
            return
        reset_target = SUBTYPE_RESET[subtype]
        _log.info(f'reset to {reset_target} {subtype}')
        self._driver.publish(self._path + '/h/!reset', reset_target)
        self._driver.close(self._path)
        path = self._path_app if subtype == SUBTYPE_CTRL_APP else self._path_updater
        self._device_await(path)
        self._running_subtype = subtype
        self._driver.open(self._path, mode='restore')

    def segment_program(self, segment, progress, progress_msg, p1, p2):
        subtype = segment['subtype']
        target = TARGETS[subtype]
        _log.info('segment_program %s', target['name'])
        for attempt in range(10):
            attempt_msg = '' if attempt == 0 else f' (retry {attempt})'
            progress(p1, progress_msg + attempt_msg)
            if subtype == SUBTYPE_CTRL_UPDATER1:
                self.reset_to(SUBTYPE_CTRL_UPDATER2)
            elif subtype == SUBTYPE_CTRL_UPDATER2:
                self.reset_to(SUBTYPE_CTRL_UPDATER1)

            progress(p1, progress_msg + ' erase' + attempt_msg)
            self.publish(target['mem_region'] + '/!erase', 0, timeout=10)
            progress(p2, progress_msg + ' write' + attempt_msg)
            self.publish(target['mem_region'] + '/!write', segment['img'], timeout=10)
            reset_to = SUBTYPE_RESET[subtype]
            if reset_to is None:
                return None
            else:
                self.reset_to(subtype)
                version = self.version
                if self.target_id == target['target_id'] and version == segment['version']:
                    return version
        raise RuntimeError(f'Failed to program {target["name"]}')

    def query(self, topic):
        return self._driver.query(self._path + '/' + topic)

    def publish(self, topic, value, timeout=None):
        return self._driver.publish(self._path + '/' + topic, value, timeout=timeout)

    def open(self, mode=None, timeout=None):
        return self._driver.open(self._path, mode=mode, timeout=timeout)

    def close(self, timeout=None):
        return self._driver.close(self._path, timeout=timeout)


def release_program(driver: Driver, device_path: str, image: bytes,
                    force_program=None, progress=None):
    """Program a device with an official release.

    :param driver: The driver instance.
    :param device_path: The device path string for the target device.
    :param image: The binary release image to program.  See
        :func:`pyjoulescope_driver.release.release_get`.
    :param force_program: Force device programming for all segments.
        Normally, programming for segments with matching versions are skipped.
    :param progress: An optional callable(completion: float, msg: str) callback.
        When provided, this function will be called to provide progress updates.
        * completion: The completion status from 0.0 (not started) to 1.0 (done).
        * msg: A user-meaningful status message.
    """
    if progress is None:
        progress = lambda x, y: None
    segments = release_to_segments(image)
    progress(0.00, 'Check app version')
    programmer = Programmer(driver, device_path)
    if programmer.is_in_app:
        ctrl_app_version = programmer.version
    else:
        ctrl_app_version = None

    progress(0.02, 'Check updater2 version')
    try:
        programmer.reset_to(SUBTYPE_CTRL_UPDATER2)
        update2_version = programmer.version
    except TimeoutError:
        update2_version = None
        programmer.close()
        programmer.open()

    progress(0.05, 'Check updater1 version')
    try:
        programmer.reset_to(SUBTYPE_CTRL_UPDATER1)
        update1_version = programmer.version
    except TimeoutError:
        update1_version = None
        programmer.close()
        programmer.open()

    sensor_fpga_version = programmer.query('s/fpga/version')
    versions_before = [
        ['app', version_to_str(ctrl_app_version)],
        ['fpga', version_to_str(sensor_fpga_version)],
        ['update1', version_to_str(update1_version)],
        ['update2', version_to_str(update2_version)],
    ]
    v = ', '.join(['='.join(x) for x in versions_before])
    _log.info('Existing versions: %s', v)

    msg = 'Program updater2'
    progress(0.1, msg)
    segment = segments.get(SUBTYPE_CTRL_UPDATER2)
    if segment is not None and (force_program or segment['version'] != update2_version):
        update2_version = programmer.segment_program(segment, progress, msg, 0.1, 0.15)

    msg = 'Program updater1'
    progress(0.2, msg)
    segment = segments.get(SUBTYPE_CTRL_UPDATER1)
    if segment is not None and (force_program or segment['version'] != update1_version):
        update1_version = programmer.segment_program(segment, progress, msg, 0.2, 0.25)

    msg = 'Program sensor FPGA'
    progress(0.3, msg)
    segment = segments.get(SUBTYPE_SENSOR_FPGA)
    if segment is not None and (force_program or segment['version'] != sensor_fpga_version):
        programmer.segment_program(segment, progress, msg, 0.3, 0.55)

    msg = 'Program controller application'
    progress(0.65, msg)
    segment = segments.get(SUBTYPE_CTRL_APP)
    if segment is not None and (force_program or segment['version'] != ctrl_app_version):
        ctrl_app_version = programmer.segment_program(segment, progress, msg, 0.65, 0.9)
    else:
        programmer.reset_to(SUBTYPE_CTRL_APP)
    progress(0.97, 'Finalizing')
    sensor_fpga_version = programmer.query('s/fpga/version')
    programmer.close()
    
    versions_after = [
        ['app', version_to_str(ctrl_app_version)],
        ['fpga', version_to_str(sensor_fpga_version)],
        ['update1', version_to_str(update1_version)],
        ['update2', version_to_str(update2_version)],
    ]
    v = ', '.join(['='.join(x) for x in versions_after])
    _log.info('Updated to versions: %s', v)
    progress(1.0, 'Complete')
    return versions_before, versions_after

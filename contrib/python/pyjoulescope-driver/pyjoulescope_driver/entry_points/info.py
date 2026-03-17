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

from pyjoulescope_driver import Driver, __version__
import numpy as np
import os
import platform
import psutil
import sys


def parser_config(p):
    """Joulescope info."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    p.add_argument('--open', '-o',
                   choices=['defaults', 'restore'],
                   default='defaults',
                   help='The device open mode.  Defaults to "defaults".')
    p.add_argument('device_path',
                   nargs='*',
                   help='The target device for this command. ' +
                        'Provide "*" to show details for all connected devices.')
    return on_cmd


def version_to_str(version):
    if isinstance(version, str):
        return version
    v_patch = version & 0xffff
    v_minor = (version >> 16) & 0xff
    v_major = (version >> 24) & 0xff
    return f'{v_major}.{v_minor}.{v_patch}'


def _sys_info():
    try:
        from pyjls import __version__ as jls_version
    except ImportError:
        jls_version = 'uninstalled'
    try:
        os.environ['JOULESCOPE_BACKEND'] = 'none'
        from joulescope import __version__ as joulescope_version
    except ImportError:
        joulescope_version = 'uninstalled'

    cpufreq = psutil.cpu_freq()
    vm = psutil.virtual_memory()
    vm_available = (vm.total - vm.used) / (1024 ** 3)
    vm_total = vm.total / (1024 ** 3)
    return f"""\

    SYSTEM INFORMATION
    ------------------
    python               {sys.version}
    python impl          {platform.python_implementation()}
    platform             {platform.platform()}
    processor            {platform.processor()}
    CPU cores            {psutil.cpu_count(logical=False)} physical, {psutil.cpu_count(logical=True)} total
    CPU frequency        {cpufreq.current:.0f} MHz ({cpufreq.min:.0f} MHz min to {cpufreq.max:.0f} MHz max)   
    RAM                  {vm_available:.1f} GB available, {vm_total:.1f} GB total ({vm_available/vm_total *100:.1f}%)
    
    PYTHON PACKAGE INFORMATION
    --------------------------
    jls                  {jls_version}
    joulescope           {joulescope_version}
    numpy                {np.__version__}
    pyjoulescope_driver  {__version__}
    """


_JOULESCOPE_INFORMATION = """
    JOULESCOPE INFORMATION
    ----------------------"""


def _list_devices(driver):
    txt = []
    device_paths = driver.device_paths()
    if len(device_paths) is None:
        txt.append('No connected Joulescopes found')
    else:
        for device_path in device_paths:
            try:
                driver.open(device_path, mode='restore')
            except Exception:
                txt.append(f'    {device_path}: could not open')
                continue
            try:
                if '/js220/' in device_path:
                    fw = version_to_str(driver.query(f'{device_path}/c/fw/version'))
                    hw = version_to_str(driver.query(f'{device_path}/c/hw/version'))
                    fpga = version_to_str(driver.query(f'{device_path}/s/fpga/version'))
                    txt.append(f'    {device_path}: hw={hw}, fw={fw}, fpga={fpga}')
                else:
                    txt.append(f'    {device_path}')
            except Exception:
                txt.append(f'    {device_path}: could not retrieve details')
            finally:
                driver.close(device_path)
    txt.append('')
    return '\n'.join(txt)


class Info:

    def __init__(self, device_paths):
        self._device_paths = device_paths
        self._meta = {}
        self._values = {}

    def _on_pub(self, topic, value):
        self._values[topic] = value

    def _on_metadata(self, topic, value):
        if topic[-1] == '$':
            topic = topic[:-1]
        self._meta[topic] = value

    def run(self, args):
        if not self._device_paths:
            print(_sys_info())
            print(_JOULESCOPE_INFORMATION)
            with Driver() as d:
                print(_list_devices(d))
                return 0

        with Driver() as d:
            d.log_level = args.jsdrv_log_level
            devices = d.device_paths()
            if '*' in self._device_paths:
                self._device_paths = devices
            for device_path in self._device_paths:
                if device_path not in devices:
                    print(f'{device_path} requested but not found')
                    continue
                self._meta.clear()
                self._values.clear()
                d.open(device_path, mode=args.open)
                fn = self._on_metadata  # use same bound method for unsubscribe
                d.subscribe(device_path, 'metadata_rsp_retain', fn)
                d.unsubscribe(device_path, fn)
                fn = self._on_pub  # use same bound method for unsubscribe
                d.subscribe(device_path, 'pub_retain', fn)
                d.unsubscribe(device_path, fn)
                if args.verbose:
                    print(f'{device_path} metadata:')
                    for key, value in self._meta.items():
                        subtopic = key[len(device_path) + 1:]
                        print(f'  {subtopic} {value}')
                print(f'{device_path} values:')
                for key, value in self._values.items():
                    meta = self._meta.get(key, None)
                    if meta is not None:
                        fmt = meta.get('format', None)
                        if fmt == 'version':
                            value = version_to_str(value)
                    subtopic = key[len(device_path) + 1:]
                    print(f'  {subtopic} = {value}')
        return 0


def on_cmd(args):
    return Info(args.device_path).run(args)

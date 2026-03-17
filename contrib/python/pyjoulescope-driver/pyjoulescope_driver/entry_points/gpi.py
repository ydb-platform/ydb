# Copyright 2022-2023 Jetperch LLC
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

from pyjoulescope_driver import Driver
import time


def parser_config(p):
    """Display Joulescope general-purpose input values."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    return on_cmd


def _query_gpi_value(d, device):
    gpi_value = None

    def on_gpi_value(topic, value):
        nonlocal gpi_value
        gpi_value = value

    d.subscribe(f'{device}/s/gpi/+/!value', 'pub', on_gpi_value)
    d.publish(f'{device}/s/gpi/+/!req', 0)
    t_start = time.time()
    while gpi_value is None:
        if time.time() - t_start > 1.0:
            raise RuntimeError('_query_gpi_value timed out')
        time.sleep(0.001)
    d.unsubscribe(f'{device}/s/gpi/+/!value', on_gpi_value)
    return gpi_value


def on_cmd(args):
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        for device in d.device_paths():
            try:
                d.open(device, 'restore')
                gpi = _query_gpi_value(d, device)
                print(f'{device}: 0x{gpi:02x}')
                d.close(device)
            except Exception:
                print(f'{device} unavailable')

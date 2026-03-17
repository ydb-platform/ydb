# Copyright 2023 Jetperch LLC
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

from pyjoulescope_driver import Driver, time64
import sys
import time


def parser_config(p):
    """Measure energy and charge over the specified duration."""
    p.add_argument('--duration',
                   default=1.0,
                   type=time64.duration_to_seconds,
                   help='The capture duration in float seconds. '
                        + 'Add a suffix for other units: s=seconds, m=minutes, h=hours, d=days')
    p.add_argument('--no-progress',
                   action='store_const',
                   default=_on_progress,
                   const=None,
                   help='Skip progress display.')
    p.add_argument('--show-only',
                   choices=['energy', 'charge'],
                   help='Show only the result in SI units, use with --no-progress')
    return on_cmd


def _on_progress(fract, message):
    # The MIT License (MIT)
    # Copyright (c) 2016 Vladimir Ignatev
    #
    # Permission is hereby granted, free of charge, to any person obtaining
    # a copy of this software and associated documentation files (the "Software"),
    # to deal in the Software without restriction, including without limitation
    # the rights to use, copy, modify, merge, publish, distribute, sublicense,
    # and/or sell copies of the Software, and to permit persons to whom the Software
    # is furnished to do so, subject to the following conditions:
    #
    # The above copyright notice and this permission notice shall be included
    # in all copies or substantial portions of the Software.
    #
    # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    # INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
    # PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
    # FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
    # OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
    # OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    fract = min(max(float(fract), 0.0), 1.0)
    bar_len = 25
    filled_len = int(round(bar_len * fract))
    percents = int(round(100.0 * fract))
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    msg = f'[{bar}] {percents:3d}% {message:40s}\r'
    sys.stdout.write(msg)
    sys.stdout.flush()


def measure(driver, device, duration=None, on_progress=None):
    """Measure energy over a duration.

    :param driver: The driver instance.
    :param device: The device topic prefix.
    :param duration: The duration in seconds.
    :param on_progress: The callable(fract, message) for progress.
    :return: dict containing keys energy, charge, duration.
    """
    duration = 30 if duration is None else int(duration)
    data = []
    if duration <= 0:
        return 0, 0
    total_count = 10 * duration + 1  # need one extra since use first as a baseline

    def _on_statistics_value(topic, value):
        data.append(value)

    driver.subscribe(device + '/s/stats/value', 'pub', _on_statistics_value)
    try:
        while len(data) < total_count:
            time.sleep(0.050)
            duration_remaining = (total_count - len(data)) / 10
            if callable(on_progress):
                on_progress(len(data) / total_count, f' {duration_remaining:.1f} seconds left')
    except KeyboardInterrupt:
        pass
    finally:
        driver.unsubscribe(device + '/s/stats/value', _on_statistics_value)

    if len(data) > total_count:
        data = data[:total_count]
    return {
        'energy': data[-1]['accumulators']['energy']['value'] - data[0]['accumulators']['energy']['value'],
        'charge': data[-1]['accumulators']['charge']['value'] - data[0]['accumulators']['charge']['value'],
        'duration': (len(data) - 1) / 10.0,
    }


def on_cmd(args):
    with Driver() as d:
        devices = d.device_paths()
        if len(devices) != 1:
            print('Found %d devices', len(devices))
            return 1
        device = devices[0]
        d.open(device)
        d.publish(device + '/s/i/range/mode', 'auto')
        d.publish(device + '/s/stats/scnt', 100_000)  # 10 Hz update rate
        d.publish(device + '/s/stats/ctrl', 1)
        data = measure(d, device, duration=args.duration, on_progress=args.no_progress)
        d.publish(device + '/s/stats/ctrl', 0)
        d.close(device)

    if args.no_progress is not None:
        sys.stdout.write('                                                            \r')
        sys.stdout.flush()
    if args.show_only == 'energy':
        print(data['energy'])
    elif args.show_only == 'charge':
        print(data['charge'])
    else:
        print(f"energy={data['energy']} J")
        print(f"charge={data['charge']} C")
        print(f"duration={data['duration']} s")
        print(f"current_average={data['charge'] / data['duration']} A")
        print(f"power_average={data['energy'] / data['duration']} W")
    return 0

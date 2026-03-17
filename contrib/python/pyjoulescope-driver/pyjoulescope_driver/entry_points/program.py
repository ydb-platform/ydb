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

from pyjoulescope_driver import Driver
from pyjoulescope_driver.program import release_program
from pyjoulescope_driver.release import release_get
import sys


def parser_config(p):
    """Program the Joulescope JS220 firmware and gateware."""
    p.add_argument('--maturity', '-m',
                   default='stable',
                   help='Maturity target to program which is one of alpha, beta, stable.')
    p.add_argument('--device-path',
                   help='The target device for this command.')
    p.add_argument('--force-download',
                   action='store_true',
                   help='Force release download.')
    p.add_argument('--force-program',
                   action='store_true',
                   help='Force segment programmer regardless of existing versions.')
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


def on_cmd(args):
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        device_paths = d.device_paths()
        if args.device_path is not None:
            if args.device_path not in device_paths:
                print('Device {args.device_path} not found in {device_paths}')
                return 1
            device_path = args.device_path
        elif len(device_paths) == 0:
            print('No device found')
            return 1
        elif len(device_paths) > 1:
            print('Multiple devices found.  Use "--device-path" to specify the desired device from:')
            print(f'{device_paths}')
            return 1
        else:
            device_path = device_paths[0]
        d.open(device_path)
        image = release_get(args.maturity, force_download=args.force_download)
        rv = release_program(d, device_path, image,
                             force_program=args.force_program,
                             progress=_on_progress)
        versions_before = dict(rv[0])
        print('\nProgramming completed:')
        for key, value in rv[1]:
            v = versions_before.get(key, '?.?.?')
            print(f'    {key:10s}  {v} => {value}')
    return 0

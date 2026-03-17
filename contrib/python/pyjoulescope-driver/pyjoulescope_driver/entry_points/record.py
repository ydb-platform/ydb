# Copyright 2023-2024 Jetperch LLC
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

from pyjoulescope_driver import Driver, Record, time64
import time


def parser_config(p):
    """Capture streaming samples to a JLS v2 file."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    p.add_argument('--duration',
                   type=time64.duration_to_seconds,
                   help='The capture duration in float seconds. '
                        + 'Add a suffix for other units: s=seconds, m=minutes, h=hours, d=days')
    p.add_argument('--frequency', '-f',
                   type=int,
                   help='The sampling frequency in Hz.')
    p.add_argument('--open', '-o',
                   choices=['defaults', 'restore'],
                   default='defaults',
                   help='The device open mode.  Defaults to "defaults".')
    p.add_argument('--serial_number',
                   help='The serial number of the Joulescope for this capture.')
    p.add_argument('--set',
                   default=[],
                   action='append',
                   help='Set a parameter using "topic=value"')
    p.add_argument('--signals',
                   default='current,voltage',
                   help='The comma-separated list of signals to capture which include '
                        + 'current, voltage, power, current_range, gpi[0], gpi[1], gpi[2], gpi[3], trigger_in. '
                        + 'You can also use the short form i, v, p, r, 0, 1, 2, 3, T '
                        + 'Defaults to current,voltage.')
    p.add_argument('--note',
                   help='Add an arbitrary note to the JLS file. '
                        + 'Provide a quoted string to handle spaces.')
    p.add_argument('filename',
                   nargs='?',
                   default=time64.filename(),
                   help='The JLS filename to record. '
                        + 'If not provided, construct filename from current time.')
    return on_cmd


def on_cmd(args):
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        if args.serial_number is not None:
            device_paths = [p for p in d.device_paths() if p.lower().endswith(args.serial_number.lower())]
        else:
            device_paths = d.device_paths()
        if len(device_paths) == 0:
            print('Device not found')
            return

        try:
            for device_path in device_paths:
                if args.verbose:
                    print(f'Open device: {device_path}')
                d.open(device_path, mode=args.open)
                try:  # configure the device
                    fs = args.frequency
                    if fs is None:
                        fs = 2_000_000 if 'js110' in device_path else 1_000_000
                    else:
                        fs = int(fs)
                    d.publish(f'{device_path}/h/fs', fs)
                except Exception:
                    print('failed to configure device')
                    return 1

                if args.open == 'defaults':
                    if 'js110' in device_path:
                        d.publish(f'{device_path}/s/i/range/select', 'auto')
                        d.publish(f'{device_path}/s/v/range/select', '15 V')
                    elif 'js220' in device_path:
                        d.publish(f'{device_path}/s/i/range/mode', 'auto')
                        d.publish(f'{device_path}/s/v/range/mode', 'auto')
                    else:
                        print(f'Unsupported device {device_path}')

                for set_cmd in args.set:
                    topic, value = set_cmd.split('=')
                    d.publish(f'{device_path}/{topic}', value)

            wr = Record(d, device_paths, args.signals)
            if args.verbose:
                print(f'Record to file: {args.filename}')
            print('Start recording.  Press CTRL-C to stop.')
            user_data = []
            if args.note is not None:
                user_data.append([0, args.note])
            wr.open(args.filename, user_data=user_data)
            t_stop = None if args.duration is None else time.time() + args.duration
            try:
                while t_stop is None or t_stop > time.time():
                    time.sleep(0.010)
                if args.verbose:
                    print(f'Record complete due to duration')
            except KeyboardInterrupt:
                if args.verbose:
                    print(f'Record complete due to user CTRL-C')
            finally:
                wr.close()
        finally:
            for device_path in device_paths:
                if args.verbose:
                    print(f'Close device: {device_path}')
                d.close(device_path)
    if args.verbose:
        print(f'Record complete')
    return 0

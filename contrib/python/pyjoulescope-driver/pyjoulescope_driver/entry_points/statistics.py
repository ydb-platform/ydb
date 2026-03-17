# Copyright 2022-2024 Jetperch LLC
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
    """Display Joulescope measurement statistics."""
    p.add_argument('--js110_host',
                   action='store_true',
                   help='Use JS110 host-side statistics (on-instrument by default).')
    p.add_argument('--frequency',
                   default=2.0,
                   type=float,
                   help='The desired statistics frequency.')
    p.add_argument('--duration',
                   type=time64.duration_to_seconds,
                   help='The capture duration in float seconds. '
                        + 'Add a suffix for other units: s=seconds, m=minutes, h=hours, d=days')
    p.add_argument('device_path',
                   nargs='*',
                   help='The target device for this command.')
    return on_cmd


def _on_statistics_value(topic, value):
    device = topic.split('/s/')[0]
    sample_id = value['time']['samples']['value'][0]
    i_avg, i_std, i_min, i_max = [value['signals']['current'][x]['value'] for x in ['avg', 'std', 'min', 'max']]
    v_avg, v_std, v_min, v_max = [value['signals']['voltage'][x]['value'] for x in ['avg', 'std', 'min', 'max']]
    p_avg, p_std, p_min, p_max = [value['signals']['power'][x]['value'] for x in ['avg', 'std', 'min', 'max']]
    charge = value['accumulators']['charge']['value']
    energy = value['accumulators']['energy']['value']
    print(f'{device},{sample_id},' +
          f'{i_avg},{i_std},{i_min},{i_max},' +
          f'{v_avg},{v_std},{v_min},{v_max},' +
          f'{p_avg},{p_std},{p_min},{p_max},' +
          f'{charge},{energy}')


def on_cmd(args):
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        devices = d.device_paths()
        if len(args.device_path):
            for device in args.device_path:
                if device not in devices:
                    print(f'device {device} not found')
                    return 1
            devices = args.device_path
        # display the column header
        print("#device,sampled_id," +
            "i_avg,i_std,i_min,i_max," +
            "v_avg,v_std,v_min,v_max," +
            "p_avg,p_std,p_min,p_max," +
            "charge,energy")
        for device in devices:
            d.open(device)
            if 'js110' in device:
                d.publish(device + '/s/i/range/select', 'auto')
                if args.js110_host:
                    # JS110 with host-side statistics
                    d.publish(device + '/s/i/ctrl', 'on')
                    d.publish(device + '/s/v/ctrl', 'on')
                    d.publish(device + '/s/p/ctrl', 'on')
                    scnt = int(round(2_000_000 / args.frequency))
                    d.publish(device + '/s/stats/scnt', scnt)
                    d.publish(device + '/s/stats/ctrl', 'on')
                    d.subscribe(device + '/s/stats/value', 'pub', _on_statistics_value)
                else:
                    # JS110 with sensor-side statistics (no standard deviation), fixed at 2 Hz
                    d.subscribe(device + '/s/sstats/value', 'pub', _on_statistics_value)
            elif 'js220' in device:
                # JS220, always sensor-side statistics
                d.publish(device + '/s/i/range/mode', 'auto')
                scnt = int(round(1_000_000 / args.frequency))
                d.publish(device + '/s/stats/scnt', scnt)
                d.publish(device + '/s/stats/ctrl', 1)
                d.subscribe(device + '/s/stats/value', 'pub', _on_statistics_value)
            else:
                print(f'Skip unsupported device {device}')
        try:
            t_start = time.time()
            while True:
                if args.duration is not None and (time.time() - t_start) >= args.duration:
                    break
                time.sleep(0.025)
        except KeyboardInterrupt:
            sys.stdout.flush()

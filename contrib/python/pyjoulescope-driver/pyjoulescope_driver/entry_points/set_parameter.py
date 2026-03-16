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


"""
Joulescope JS220 examples

Blinking led (override normal operation)
python -m pyjoulescope_driver set u/js220/000000 c/led/en=1 c/led/red=0x18 c/led/green=0x0E c/led/blue=3 s/led/en=1 s/led/red=0xC0 s/led/green=0x30 s/led/blue=0x0C

Restore normal led operation:
python -m pyjoulescope_driver set u/js220/000000 c/led/en=0 s/led/en=0

Restore all defaults:
python -m pyjoulescope_driver set u/js220/000000 --open defaults

"""

NAME = 'set'

def _args_validate(v):
    topic, value = v.split('=')
    try:
        if value.startswith('0x'):
            value = int(value[2:], 16)
        else:
            value = int(value)
    except ValueError:
        pass
    return [topic, value]


def parser_config(p):
    """Set Joulescope parameter values."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    p.add_argument('--open', '-o',
                   choices=['defaults', 'restore'],
                   default='restore',
                   help='The device open mode.  Defaults to "restore".')
    p.add_argument('device_path',
                   help='The target device for this command.')
    p.add_argument('args',
                   type=_args_validate,
                   nargs='*',
                   help='topic=value pairs.')
    return on_cmd


def on_cmd(args):
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        d.open(args.device_path, mode=args.open)
        for topic, value in args.args:
            if not topic.startswith(args.device_path):
                topic = f'{args.device_path}/{topic}'
            print(f'{topic} {value}')
            d.publish(topic, value)
    return 0

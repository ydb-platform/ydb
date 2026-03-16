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
import threading
import time


_quit = False


def _validate_timeout(v):
    v = float(v)
    if v == 0 or v >= 0.001:
        return v
    raise ValueError(f'Invalid timeout {v}')


def parser_config(p):
    """Threading demonstration for the pyjoulescope_driver.

    For example:
        python -m pyjoulescope_driver --log_level INFO --jsdrv_log_level info api_timeout --threads 10 --duration 10 --timeout 0.1
    """
    p.add_argument('--duration', '-d',
                   type=float,
                   default=1.0,
                   help='The duration in seconds to run the threading demonstration.')
    p.add_argument('--threads',
                   type=int,
                   default=10,
                   help='The number of threads to run simultaneously for API timeout testing.')
    p.add_argument('--timeout', '-t',
                   type=_validate_timeout,
                   default=0.25,
                   help='The API timeout in seconds.')
    return on_cmd


def _thread_run(state):
    global _quit
    index = state['index']
    d = state['driver']
    counter = 0
    while not _quit:
        try:
            d.publish('@/timeout', 0, timeout=state['timeout'])
            if state['timeout']:
                print('No timeout when expected')
                return 1
        except TimeoutError:
            pass  # expected
        counter += 1
    state['counter'] = counter
    print(f'{index} {counter}')


def on_cmd(args):
    global _quit
    with Driver() as d:
        d.log_level = args.jsdrv_log_level
        threads = []
        for idx in range(args.threads):
            state = {
                'name': f'jsdrv {idx}',
                'index': idx,
                'driver': d,
                'timeout': args.timeout,
                'counter': 0,
            }
            state['thread'] = threading.Thread(name=state['name'], target=_thread_run, args=[state])
            state['thread'].start()
            threads.append(state)

        t_end = time.time() + args.duration
        while time.time() < t_end:
            time.sleep(0.010)

        _quit = True
        counter = 0
        for thread in threads:
            thread['thread'].join()
            counter += thread['counter']
        print(f'Total calls: {counter}')

    return 0

# Copyright 2021-2022 Jetperch LLC
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

from pyjls import Reader
import logging
import textwrap

log = logging.getLogger(__name__)


def parser_config(p):
    """JLS file info."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    p.add_argument('--utc',
                   action='store_true',
                   help='Display the UTC data for each channel.')
    p.add_argument('filename',
                   help='JLS filename')
    return on_cmd


def _user_data_cbk(chunk_meta, data):
    suffix = ''
    if len(data) > 64:
        data = data[:64]
        suffix = '...'
    print(f'    {chunk_meta}: {data}{suffix}')


def on_cmd(args):
    with Reader(args.filename) as r:
        print('Sources:')
        for source in r.sources.values():
            s = source.info(verbose=args.verbose)
            print(textwrap.indent(s, "    "))
        print('Signals:')
        for signal in r.signals.values():
            s = signal.info(verbose=args.verbose)
            print(textwrap.indent(s, "    "))
            if args.utc:
                def on_utc(entries):
                    for sample_id, timestamp in entries:
                        print(f'       {sample_id}, {timestamp}')
                r.utc(signal.signal_id, -signal.sample_rate, on_utc)

        print('User Data:')
        r.user_data(_user_data_cbk)


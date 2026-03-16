# Copyright 2025 Jetperch LLC
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


from pyjls import Reader, Writer, time64
import os
import sys


_CHUNK_SIZE = 10_000_000   # in samples


def _on_progress(fract, message=None):
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
    message = '' if message is None else str(message)
    fract = min(max(float(fract), 0.0), 1.0)
    bar_len = 25
    filled_len = int(round(bar_len * fract))
    percents = int(round(100.0 * fract))
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    msg = f'[{bar}] {percents:3d}% {message:40s}\r'
    sys.stdout.write(msg)
    sys.stdout.flush()


def parser_config(p):
    """Add or override FSR signal UTC time."""
    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information')
    p.add_argument('--timestamp',
                   help='new starting timestamp as ISO 8601: YYYY-MM-DDThh:mm:ss.ffffff')
    p.add_argument('--no-progress',
                   action='store_true',
                   help='Hide progress bar.')
    p.add_argument('infile',
                   help='JLS input file path')
    p.add_argument('outfile',
                   help='JLS output file path')
    return on_cmd


def on_cmd(args):
    on_progress = _on_progress if not args.no_progress else lambda x: None

    with Reader(args.infile) as r:
        with Writer(args.outfile) as w:
            for source in r.sources.values():
                if source.source_id == 0:
                    continue
                w.source_def_from_struct(source)
            for signal in r.signals.values():
                if signal.signal_id == 0:
                    continue
                w.signal_def_from_struct(signal)

            if args.timestamp is None:
                timestamp = os.path.getctime(args.infile)
            else:
                try:
                    timestamp = int(args.timestamp)
                except ValueError:
                    timestamp = args.timestamp  # presume ISO string
            timestamp = time64.as_time64(timestamp)

            signals = [x for x in r.signals.values() if x.signal_type == 0]
            signal_count = len(signals)

            for signal_idx, signal in enumerate(signals):
                offset = 0
                length = signal.length
                while length:
                    k = min(length, _CHUNK_SIZE)
                    d = r.fsr(signal.signal_id, offset, k)
                    w.fsr(signal.signal_id, offset, d)
                    offset += k
                    length -= k
                    on_progress(offset / (signal.length * signal_count) + signal_idx/signal_count)
                w.utc(signal.signal_id, 0, timestamp)
    if not args.no_progress:
        print()
    return 0

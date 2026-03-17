# Copyright 2021-2025 Jetperch LLC
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

from pyjls import Reader, SignalType, DataType, time64
import logging
import numpy as np

log = logging.getLogger(__name__)


def parser_config(p):
    """Extract aligned points from a JLS file to a CSV file."""
    p.add_argument('--count',
                   type=int,
                   default=10000,
                   help='The number of CSV rows (points) to export.')
    p.add_argument('--signals',
                   help='The comma-separate list of signal names or ids to display.  Default extracts all.')
    p.add_argument('infile',
                   help='JLS input filename')
    p.add_argument('outfile',
                   help='The output CSV filename.')
    return on_cmd


_FORMAT = {
    DataType.U1: '%.3f',
    DataType.U4: '%.3f',
    DataType.U8: '%.3f',
    DataType.U16: '%.3f',
    DataType.U32: '%.3f',
    DataType.U64: '%.3f',
    DataType.I4: '%.3f',
    DataType.I8: '%.3f',
    DataType.I16: '%.3f',
    DataType.I32: '%.3f',
    DataType.I64: '%.3f',
    DataType.F32: '%+9e',
    DataType.F64: '%+15e',
}


def on_cmd(args):
    columns = ['timestamp']
    with Reader(args.infile) as r:
        if args.signals is not None:
            signals = [r.signal_lookup(s) for s in args.signals.split(',')]
        else:
            signals = [s for s in r.signals.values()]
        signals = [s for s in signals if s.signal_type == SignalType.FSR]

        # find the earliest starting time for any signal (time64)
        t_start = max([r.sample_id_to_timestamp(signal.signal_id, 0) for signal in signals])
        t_end = max([r.sample_id_to_timestamp(signal.signal_id, signal.length - 1) for signal in signals])

        fmt = ['%.6f']
        count = [args.count]
        signal_info = []
        for signal in signals:
            fmt.append(_FORMAT.get(signal.data_type, '%g'))
            columns.append(f'{r.sources[signal.source_id].name}.{signal.name}')
            s_start = r.timestamp_to_sample_id(signal.signal_id, t_start)
            s_end = r.timestamp_to_sample_id(signal.signal_id, t_end)
            signal_info.append([s_start, s_end])
            count.append(s_end - s_start)
        count = min(count)
        data = np.empty((count, 1 + len(signals)), dtype=float)
        data[:, 0] = np.linspace(0, (t_end - t_start) / time64.SECOND, count, dtype=np.float64)

        for idx, signal in enumerate(signals):
            s_start, s_end = signal_info[idx]
            s_len = s_end - s_start
            incr = s_len // count
            s_data = r.fsr_statistics(signal.signal_id, s_start, incr, count)
            data[:, idx + 1] = s_data[:, 0]

    np.savetxt(args.outfile, data, header=','.join(columns), delimiter=',', fmt=fmt)
    return 0

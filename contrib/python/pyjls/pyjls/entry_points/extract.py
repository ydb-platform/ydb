#!/usr/bin/env python3
# Copyright 2024 Jetperch LLC
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

"""Extract data from a JLS file into a new JLS file."""

from pyjls import Reader, Writer, time64
from datetime import datetime
import sys


def parser_config(p):
    """Extract data from a JLS file into a new JLS file."""

    p.add_argument('--verbose', '-v',
                   action='store_true',
                   help='Display verbose information.')
    p.add_argument('--sources',
                   help='The JLS sources to extract as either source_id or source name. '
                        + 'If not provided, extract all. ')
    p.add_argument('--signals',
                   help='The JLS signals to extract.  If not provided, extract all. '
                        + 'Signals may be specified in any of the following formats: '
                        + 'signal_name, signal_id, '
                        + 'source_id.signal_name, source_name.signal_name, '
                        + 'source_id.signal_id, source_name.signal_id')
    p.add_argument('--start',
                   help='The starting time in either\n' +
                        '1. ISO 8601 format such as YYYYMMDDThhssmm.ffffffZ\n' +
                        '2. Offset in float seconds or suffix for other units: s=seconds, m=minutes, h=hours, d=days')
    p.add_argument('--stop',
                   help='The ending time in either\n' +
                        '1. ISO 8601 format such as YYYYMMDDThhssmm.ffffffZ\n' +
                        '2. Offset in float seconds or suffix for other units: s=seconds, m=minutes, h=hours, d=days')
    p.add_argument('--duration',
                   type=time64.duration_to_seconds,
                   help='The export duration, which defaults to units of float seconds. '
                        + 'Add a suffix for other units: s=seconds, m=minutes, h=hours, d=days. '
                        + 'If not specified, use the entire file.')
    p.add_argument('--block-size', '--block_size',
                   default=1_000_000,
                   type=int,
                   help='The copy block size in samples.  Defaults to 1,000,000.')
    p.add_argument('input',
                   help='The input filename path.')
    p.add_argument('output',
                   help='The output filename path.')
    return on_cmd


def on_cmd(args):
    def error(msg):
        sys.stderr.write(f'ERROR: {msg}\n')
        sys.stderr.flush()
        return 1

    def verbose(msg):
        if args.verbose:
            print(msg)

    if args.stop is not None and args.duration is not None:
        return error('Provide either "--stop" or "--duration", not both')
    r = Reader(args.input)

    verbose('Filter sources')
    sources = {}
    valid_sources = None if args.sources is None else set(args.sources.split(','))
    for source in r.sources.values():
        if source.source_id == 0:
            continue
        variations = [str(source.source_id), source.name]
        if valid_sources is None or not valid_sources.isdisjoint(variations):
            sources[source.source_id] = source

    verbose('Filter signals')
    t_start = []
    t_stop = []
    signals = {}
    if args.signals is None:
        valid_signal_ids = None
    else:
        valid_signal_ids = [r.signal_lookup(s).signal_id for s in set(args.signals.split(','))]
    for signal in r.signals.values():
        if signal.source_id not in sources:
            continue
        if valid_signal_ids is None or not signal.signal_id not in valid_signal_ids:
            signals[signal.signal_id] = signal
            t_start.append(r.sample_id_to_timestamp(signal.signal_id, 0))
            t_stop.append(r.sample_id_to_timestamp(signal.signal_id, signal.length - 1))
    if not len(signals):
        return error('No matching signals found')

    verbose('Determine time range')
    t64_start_limit = max(t_start)
    t64_stop_limit = min(t_stop)
    if args.start is None:
        t64_start = t64_start_limit
    else:
        try:
            t64_start = time64.as_time64(datetime.fromisoformat(args.start))
        except Exception:
            t64_start = t64_start_limit + int(time64.duration_to_seconds(args.start) * time64.SECOND)
    if args.duration is not None:
        t64_stop = t64_start + int(args.duration * time64.SECOND)
    elif args.stop is None:
        t64_stop = t64_stop_limit
    else:
        try:
            t64_stop = time64.as_time64(datetime.fromisoformat(args.stop))
        except Exception:
            t64_stop = t64_start_limit + int(time64.duration_to_seconds(args.stop) * time64.SECOND)
    t64_start_limit_str = time64.as_datetime(t64_start_limit).isoformat()
    t64_stop_limit_str = time64.as_datetime(t64_stop).isoformat()
    t_start_str = time64.as_datetime(t64_start).isoformat()
    t_stop_str = time64.as_datetime(t64_stop).isoformat()
    if not t64_start_limit <= t64_start < t64_stop_limit:
        return error(f'Start {t_start_str} out of range: {t64_start_limit_str} to {t64_stop_limit_str}')
    if not t64_start_limit < t64_stop <= t64_stop_limit:
        return error(f'Stop {t_stop_str} out of range: {t64_start_limit_str} to {t64_stop_limit_str}')
    verbose(f'Time range: {t_start_str} to {t_stop_str}')

    sources_used = set([s.source_id for s in signals.values()])
    signals_used = set([s.signal_id for s in signals.values()])
    verbose(f'sources: {sources_used}')
    verbose(f'signals: {signals_used}')

    verbose('Create JLS file')
    w = Writer(args.output)
    for source_id in sources_used:  # Create sources
        w.source_def_from_struct(sources[source_id])
    try:  # Create signals and copy data
        for signal_id in signals_used:
            w.signal_def_from_struct(signals[signal_id])
            offset = r.timestamp_to_sample_id(signal_id, t64_start)
            offset_end = r.timestamp_to_sample_id(signal_id, t64_stop)
            signal_name = f'{sources[signals[signal_id].source_id].name}.{signals[signal_id].name}'
            verbose(f'Copy signal {signal_id} {signal_name:30s} {offset_end - offset + 1} samples')
            w.utc(signal_id, offset, t64_start)
            offset_end += 1  # include the last sample
            while offset < offset_end:
                block_size = min(offset_end - offset, args.block_size)
                data = r.fsr(signal_id, offset, block_size)
                w.fsr(signal_id, offset, data)
                offset += block_size
            offset -= 1  # adjust to last sample_id
            w.utc(signal_id, offset, r.sample_id_to_timestamp(signal_id, offset))
    finally:
        w.close()
    verbose('Export complete')
    return 0

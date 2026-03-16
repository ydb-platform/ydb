# Copyright 2021-2024 Jetperch LLC
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

from pyjls import Reader, Writer, SourceDef, SignalDef, AnnotationType
import logging
import numpy as np
import os

log = logging.getLogger(__name__)


def parser_config(p):
    """Generate a JLS annotation file."""
    p.add_argument('in_file',
                   help='The input JLS file path')
    p.add_argument('--name',
                   default='anno',
                   help='The annotation file subname')
    return on_cmd


def on_cmd(args):
    path = os.path.dirname(args.in_file)
    fname = os.path.basename(args.in_file)
    fbase, fext = os.path.splitext(fname)
    fout = os.path.join(path, f'{fbase}.{args.name}{fext}')

    with Writer(fout) as w:
        with Reader(args.in_file) as r:
            sources: list[SourceDef] = list(r.sources.values())
            signals: list[SignalDef] = list(r.signals.values())
            for source in sources:
                if source.source_id:
                    w.source_def_from_struct(source)
            for signal in signals:
                if signal.signal_id:
                    w.signal_def_from_struct(signal)
                    for sample_id in [0, signal.length - 1]:
                        t_utc = r.sample_id_to_timestamp(signal.signal_id, sample_id)
                        w.utc(signal.signal_id, sample_id, t_utc)
        signal = signals[1]  # for now
        generator = np.random.default_rng()
        timestamps = generator.integers(0, signal.length, 12)
        timestamps = sorted(timestamps)
        event_strs = ['on', 'off', 'start', 'stop', 'off by 1']
        marker_idx = 1
        for idx, timestamp in enumerate(timestamps):
            anno_type = idx % 4
            if anno_type <= 1:
                event_str = np.random.choice(event_strs)
                w.annotation(signal.signal_id, timestamp, 0, AnnotationType.TEXT, 0, event_str)
            elif anno_type == 2:
                w.annotation(signal.signal_id, timestamp, 0, AnnotationType.VMARKER, 0, str(marker_idx))
                marker_idx += 1
            elif anno_type == 3:
                w.annotation(signal.signal_id, timestamp, 0, AnnotationType.VMARKER, 0, f'{marker_idx}a')
                w.annotation(signal.signal_id, timestamp + 100, np.nan, AnnotationType.VMARKER, 0, f'{marker_idx}b')
                marker_idx += 1

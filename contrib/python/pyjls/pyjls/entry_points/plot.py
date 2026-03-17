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

from pyjls import Reader, SignalType, SummaryFSR, time64
import logging
import numpy as np
import sys

log = logging.getLogger(__name__)


def parser_config(p):
    """Plot JLS file data contents for FSR signals."""
    p.add_argument('--out',
                   help='The output filename path.')
    p.add_argument('--show',
                   action='store_true',
                   help='Display the plot.')
    p.add_argument('--sample_count',
                   type=int,
                   default=1000,
                   help='The number of samples to display')
    p.add_argument('--hide_min_max', '--hide-min-max',
                   action='store_true',
                   help='Hide the min/max shaded region.')
    p.add_argument('--signals',
                   help='The comma-separate list of signal names or ids to display.  Default shows all.')
    p.add_argument('infile',
                   help='JLS input filename')
    return on_cmd


def on_cmd(args):
    try:
        import matplotlib.pyplot as plt
    except (ModuleNotFoundError, ImportError):
        print('Could not import matplotlib.  Install using:')
        print(f'{sys.executable} -m pip install -U matplotlib')
        return 1

    f = plt.figure()
    with Reader(args.infile) as r:
        if args.signals is not None:
            signals = [r.signal_lookup(s) for s in args.signals.split(',')]
        else:
            signals = [s for s in r.signals.values()]
        signals = [s for s in signals if s.signal_type == SignalType.FSR]
        ax1, ax = None, None

        # find the earliest starting time for any signal (time64)
        t_plot_start = min([r.sample_id_to_timestamp(signal.signal_id, 0) for signal in signals])

        for idx, signal in enumerate(signals):
            ax = f.add_subplot(len(signals), 1, idx + 1, sharex=ax1)
            if ax1 is None:
                ax.set_title(args.infile)
                ax1 = ax
            ax.grid(True)
            ax.set_ylabel(f'{signal.name} ({signal.units})')
            incr = signal.length // args.sample_count
            length = signal.length // incr
            data = r.fsr_statistics(signal.signal_id, 0, incr, length)

            # compute each sample's time relative to t_plot_start (float seconds)
            t_start = (r.sample_id_to_timestamp(signal.signal_id, 0) - t_plot_start) / time64.SECOND
            s_end = (length - 1) * incr
            t_end = (r.sample_id_to_timestamp(signal.signal_id, s_end) - t_plot_start) / time64.SECOND
            x = np.linspace(t_start, t_end, length, dtype=np.float64)

            if not args.hide_min_max:
                ax.fill_between(x, data[:, SummaryFSR.MAX], data[:, SummaryFSR.MIN], alpha=0.2)
            ax.plot(x, data[:, SummaryFSR.MEAN])
        if ax is not None:
            ax.set_xlabel('Time (seconds)')

    if args.show:
        plt.show()
    if args.out:
        f.savefig(args.out)
    return 0

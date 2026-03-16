# Copyright 2023 Jetperch LLC
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


"""
Record streaming sample data to a JLS v2 file.
"""

import copy
import numpy as np
from pyjoulescope_driver import time64
import logging


_PYJLS_VERSION_MIN = (0, 9, 5)  # inclusive
_PYJLS_VERSION_MAX = (1, 0, 0)  # exclusive


try:
    from pyjls import Writer, SignalType, DataType, __version__
    _DTYPE_MAP = {
        'f32': DataType.F32,
        'u8': DataType.U8,
        'u4': DataType.U4,
        'u1': DataType.U1,
    }
except ImportError:
    Writer = None
    _DTYPE_MAP = {}


_SIGNALS = {
    'current': {
        'signal_type': 'f32',
        'units': 'A',
        'ctrl_topic': 's/i/ctrl',
        'data_topic': 's/i/!data',
    },
    'voltage': {
        'signal_type': 'f32',
        'units': 'V',
        'ctrl_topic': 's/v/ctrl',
        'data_topic': 's/v/!data',
    },
    'power': {
        'signal_type': 'f32',
        'units': 'W',
        'ctrl_topic': 's/p/ctrl',
        'data_topic': 's/p/!data',
    },
    'current_range': {
        'signal_type': 'u4',
        'units': '',
        'ctrl_topic': 's/i/range/ctrl',
        'data_topic': 's/i/range/!data',
    },
    'gpi[0]': {
        'signal_type': 'u1',
        'units': '',
        'ctrl_topic': 's/gpi/0/ctrl',
        'data_topic': 's/gpi/0/!data',
    },
    'gpi[1]': {
        'signal_type': 'u1',
        'units': '',
        'ctrl_topic': 's/gpi/1/ctrl',
        'data_topic': 's/gpi/1/!data',
    },
    'gpi[2]': {
        'signal_type': 'u1',
        'units': '',
        'ctrl_topic': 's/gpi/2/ctrl',
        'data_topic': 's/gpi/2/!data',
    },
    'gpi[3]': {
        'signal_type': 'u1',
        'units': '',
        'ctrl_topic': 's/gpi/3/ctrl',
        'data_topic': 's/gpi/3/!data',
    },
    'trigger_in': {
        'signal_type': 'u1',
        'units': '',
        'ctrl_topic': 's/gpi/7/ctrl',
        'data_topic': 's/gpi/7/!data',
    },
}


_SIGNAL_SHORT_MAP = [
    ('current', 'i'),
    ('voltage', 'v'),
    ('power', 'p'),
    ('current_range', 'r', 'current range'),
    ('gpi[0]', '0'),
    ('gpi[1]', '1'),
    ('gpi[2]', '2'),
    ('gpi[3]', '3'),
    ('trigger_in', 'T', 't'),
]


def _signal_name_map():
    m = {}
    for z in _SIGNAL_SHORT_MAP:
        signal_name = z[0]
        m[signal_name] = signal_name
        for n in z[1:]:
            m[n] = signal_name
    return m


class Record:
    """Record streaming sample data to a JLS v2 file.

    :param driver: The active driver instance.
    :param device_path: The device prefix path or list of device prefix paths.
    :param signals: The list of signals to record.  None=['current', 'voltage']
    :param auto: Configure automatic operation.
        Provide the list of automatic operations to perform, which can be:
        * signal_enable
        * signal_disable
        None (default) is equivalent to ['signal_enable', 'signal_disable']

    Call :meth:`open` to start recording and :meth:`close` to stop.
    """

    def __init__(self, driver, device_path, signals=None, auto=None):
        if Writer is None:
            raise RuntimeError('pyjls package not found.  Install using:\n' +
                               '  pip3 install -U pyjls')
        pyjls_version = tuple([int(x) for x in __version__.split('.')])
        if pyjls_version < _PYJLS_VERSION_MIN or pyjls_version >= _PYJLS_VERSION_MAX:
            raise ImportError(f'Unsupported pyjls version {__version__}\n' +
                              f'  Require {_PYJLS_VERSION_MIN} <= pyjls version < {_PYJLS_VERSION_MAX}\n' +
                              '  pip3 install -U pyjls')
        self._utc_interval = time64.MINUTE
        self._log = logging.getLogger(__name__)
        self._wr = None
        self._data_map = {}
        self._driver = driver
        self._device_paths = [device_path] if isinstance(device_path, str) else device_path
        self._on_data_fn = self._on_data  # bind and save for unsubscribe
        if signals is None:
            signals = ['current', 'voltage']
        elif isinstance(signals, str):
            signals = [s.strip() for s in signals.split(',')]
        m = _signal_name_map()
        signals = [m[s] for s in signals]
        if auto is None:
            auto = ['signal_enable', 'signal_disable']
        if isinstance(auto, str):
            auto = [auto]
        self._auto = auto

        signal_id = 0
        self._signals = {}
        for idx, device_path in enumerate(self._device_paths):
            for signal_name in signals:
                signal_id += 1
                signal = copy.deepcopy(_SIGNALS[signal_name])
                signal['name'] = signal_name
                signal['source_id'] = idx + 1
                signal['signal_id'] = signal_id
                signal['signal_type'] = _DTYPE_MAP[signal['signal_type']]
                signal['ctrl_topic_abs'] = f"{device_path}/{signal['ctrl_topic']}"
                signal['data_topic_abs'] = f"{device_path}/{signal['data_topic']}"
                signal['utc_next'] = None
                signal['utc'] = None
                self._signals[f'{device_path}.{signal_name}'] = signal

    def open(self, filename, user_data=None):
        """Start the recording.

        :param filename: The filename for the recording.  Use
            time64.filename to produce a filename from timestamp.
        :param user_data: The list of additional user data
            given as [chunk_meta, data] pairs.
            Use chunk_meta 0 and a data string for notes
            that display in the Joulescope UI.
        :return: self.
        """
        if self._wr is not None:
            self.close()
        self._data_map.clear()
        self._wr = Writer(filename)
        if user_data is not None:
            for chunk_meta, data in user_data:
                self._wr.user_data(chunk_meta, data)
        for idx, device_path in enumerate(self._device_paths):
            _, model, serial_number = device_path.split('/')
            model = model.upper()
            self._wr.source_def(
                source_id=idx + 1,
                name=f'{model}-{serial_number}',
                vendor='Jetperch',
                model=model,
                version='',
                serial_number=serial_number,
            )

            for signal in self._signals.values():
                data_topic = signal['data_topic_abs']
                self._data_map[data_topic] = signal
                self._driver.subscribe(data_topic, ['pub'], self._on_data_fn)

            if 'signal_enable' in self._auto:
                for signal in self._signals.values():
                    ctrl_topic = signal['ctrl_topic_abs']
                    self._driver.publish(ctrl_topic, 1, timeout=0)

        return self

    def close(self):
        """Close the recording and release all resources."""
        try:
            for signal in self._signals.values():
                self._driver.unsubscribe(signal['data_topic_abs'], self._on_data_fn)
            for signal in self._signals.values():
                if signal['utc'] is not None:
                    self._wr.utc(signal['signal_id'], *signal['utc'])
                if 'signal_disable' in self._auto:
                    ctrl_topic = signal['ctrl_topic_abs']
                    try:
                        self._driver.publish(ctrl_topic, 0, timeout=0.25)
                    except TimeoutError:
                        self._log.warning('Timed out in publish: %s <= 0', ctrl_topic)
                    except Exception:
                        self._log.exception('Exception in publish: %s <= 0', ctrl_topic)
        finally:
            self._wr.close()
            self._wr = None

    def _on_data(self, topic, value):
        if self._wr is None:
            return
        signal = self._data_map[topic]
        decimate_factor = value['decimate_factor']
        signal_id = signal['signal_id']
        sample_id = value['sample_id']
        sample_id = sample_id // decimate_factor
        if signal['utc_next'] is None:
            self._wr.signal_def(
                signal_id=signal['signal_id'],
                source_id=signal['source_id'],
                signal_type=SignalType.FSR,
                data_type=signal['signal_type'],
                sample_rate=value['sample_rate'] // decimate_factor,
                name=signal['name'],
                units=signal['units'],
            )
            self._wr.utc(signal_id, sample_id, value['utc'])
            signal['utc_next'] = value['utc'] + self._utc_interval

        if value['utc'] >= signal['utc_next']:
            self._wr.utc(signal_id, sample_id, value['utc'])
            signal['utc_next'] += self._utc_interval
            signal['utc'] = None
        elif sample_id:
            signal['utc'] = (sample_id, value['utc'])

        x = value['data']
        if len(x):
            x = np.ascontiguousarray(x)
            self._wr.fsr_f32(signal_id, sample_id, x)

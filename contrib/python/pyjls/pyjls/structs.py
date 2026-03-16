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

from dataclasses import dataclass


@dataclass
class SourceDef:
    """Define a source.

    :ivar source_id: The source identifier.
    :ivar name: The source name string.
    :ivar vendor: The vendor string.
    :ivar model: The model string.
    :ivar version: The version string.
    :ivar serial_number: The serial number string.
    """
    source_id: int
    name: str = None
    vendor: str = None
    model: str = None
    version: str = None
    serial_number: str = None

    def info(self, verbose=None) -> str:
        strs = [f'{self.source_id}: {self.name}']
        if verbose:
            for field in ['vendor', 'model', 'version', 'serial_number']:
                strs.append(f'    {field}: {getattr(self, field)}')
        return '\n'.join(strs)


@dataclass
class SignalDef:
    """Define a signal.

    :ivar signal_id: The source identifier.
        0 reserved for global annotations, must be unique per instance.
    :ivar source_id: The source identifier.
        Must match a SourceDef entry.
    :ivar signal_type: The pyjls.SignalType for this signal.
    :ivar data_type: The pyjls.DataType for this signal.
    :ivar sample_rate: The sample rate per second (Hz).  0 for VSR.
    :ivar samples_per_data: The number of samples per data chunk.  (write suggestion)
    :ivar sample_decimate_factor: The number of samples per summary level 1 entry.
    :ivar entries_per_summary: The number of entries per summary chunk.  (write suggestion)
    :ivar summary_decimate_factor: The number of summaries per summary, level >= 2.
    :ivar annotation_decimate_factor: The annotation decimate factor for summaries.
    :ivar utc_decimate_factor: The UTC decimate factor for summaries.
    :ivar sample_id_offset: The sample id offset for the first sample.  (FSR only)
    :ivar name: The signal name string.
    :ivar units: The signal units string.
    :ivar length: The length in samples.
    """
    signal_id: int
    source_id: int
    signal_type: int = 0
    data_type: int = 0
    sample_rate: int = 0
    samples_per_data: int = 0
    sample_decimate_factor: int = 0
    entries_per_summary: int = 0
    summary_decimate_factor: int = 0
    annotation_decimate_factor: int = 0
    utc_decimate_factor: int = 0
    sample_id_offset: int = 0
    name: str = None
    units: str = None
    length: int = 0

    def info(self, verbose=None) -> str:
        hdr = f'{self.signal_id}: {self.source_id}.{self.name}'
        if self.signal_type == 0:
            hdr += f' ({self.length} samples at {self.sample_rate} Hz)'
        strs = [hdr]
        if verbose:
            for field in ['source_id', 'signal_type', 'data_type', 'sample_rate',
                          'samples_per_data', 'sample_decimate_factor',
                          'entries_per_summary', 'summary_decimate_factor',
                          'annotation_decimate_factor', 'utc_decimate_factor',
                          'sample_id_offset',
                          'units', 'length']:
                strs.append(f'    {field}: {getattr(self, field)}')
        return '\n'.join(strs)

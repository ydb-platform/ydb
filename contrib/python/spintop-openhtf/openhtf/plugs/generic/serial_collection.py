# Copyright 2017 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""OpenHTF plug for serial port.

Allows for writing out to a serial port.
"""

import logging
import threading

try:
  import serial
except ImportError:
  logging.error('Failed to import pyserial. Please install the `serial_collection_plug` extra, '
                'e.g. via `pip install openhtf[serial_collection_plug]`.')
  raise

import openhtf
from openhtf.util import conf


conf.declare(
    'serial_collection_port',
    description='Port on which to collect serial data.',
    default_value='/dev/ttyACM0')
conf.declare(
    'serial_collection_baud',
    description='Baud rate for serial data collection.',
    default_value=115200)


class SerialCollectionPlug(openhtf.plugs.BasePlug):
  """Plug that collects data from a serial port.

  Spawns a thread that will open the configured serial port, continuously
  poll the port for data, and write the data to the destination file as it is
  received. If any serial errors are encountered during the lifetime of the
  polling thread, data collection stops and an error message is logged.
  Otherwise, data collection stops and the serial port is closed when
  stop_collection() is called.
  """
  # Serial library can raise these exceptions
  SERIAL_EXCEPTIONS = (serial.SerialException, ValueError)

  @conf.inject_positional_args
  def __init__(self, serial_collection_port, serial_collection_baud):
    super(SerialCollectionPlug, self).__init__()
    # Instantiate the port with no name, then add the name, so it won't be
    # opened until the collection context is entered.
    self._serial = serial.Serial(port=None,
                                 baudrate=serial_collection_baud,
                                 timeout=1)
    self._serial.port = serial_collection_port
    self._collect = False
    self._collection_thread = None

  def start_collection(self, dest):
    def _poll():
      try:
        with open(dest, 'w+') as outfile:
          while self._collect:
            data = self._serial.readline().decode()
            outfile.write(data)
      except self.SERIAL_EXCEPTIONS:
        self.logger.error('Serial port error. Stopping data collection.',
                          exc_info=True)

    self._collect = True
    self._collection_thread = threading.Thread(target=_poll)
    self._collection_thread.daemon = True
    self.logger.debug(
        'Starting serial data collection on port %s.' % self._serial.port)
    self._serial.open()
    self._collection_thread.start()

  @property
  def is_collecting(self):
    if self._collection_thread is not None:
      return self._collection_thread.is_alive()
    return False

  def stop_collection(self):
    if not self.is_collecting:
      self.logger.warning(
          'Data collection was not running, cannot be stopped.')
      return
    self._collect = False
    self._collection_thread.join()
    self._serial.close()

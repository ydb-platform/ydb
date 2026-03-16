# Copyright 2016 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import time
from openhtf.plugs.usb import local_usb

class EtherSync(object):
  """EtherSync object for the access of usb device connected to
     Cambrionix unit."""

  port_map = {
      '1':'112',
      '2':'111',
      '3':'114',
      '4':'113',
      '5':'212',
      '6':'211',
      '7':'214',
      '8':'213',
  }

  def __init__(self, mac_addr):
    """Construct a EtherSync object.

      Args:
        mac_addr: mac address of the Cambrionix unit for EtherSync.
    """
    addr_info = mac_addr.lower().split(':')
    if len(addr_info) < 6:
      raise ValueError('Invalid mac address')

    addr_info[2] = 'EtherSync'
    self._addr = ''.join(addr_info[2:])

  def get_usb_serial(self, port_num):
    """Get the device serial number

    Args:
      port_num: port number on the Cambrionix unit

    Return:
      usb device serial number
    """

    port = self.port_map[str(port_num)]
    arg = ''.join(['DEVICE INFO,', self._addr, '.', port])
    cmd = (['esuit64', '-t', arg])
    info = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    serial = None
    if "SERIAL" in info:
      serial_info = info.split('SERIAL:')[1]
      serial = serial_info.split('\n')[0].strip()
      use_info = info.split('BY')[1].split(' ')[1]
      if use_info == 'NO':
        cmd = (['esuit64', '-t', 'AUTO USE ALL'])
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        time.sleep(50.0/1000.0)
    else:
      raise ValueError('No USB device detected')
    return serial

  def open_usb_handle(self, port_num):
    """open usb port

    Args:
      port_num: port number on the Cambrionix unit

    Return:
      usb handle
    """
    serial = self.get_usb_serial(port_num)
    return local_usb.LibUsbHandle.open(serial_number=serial)

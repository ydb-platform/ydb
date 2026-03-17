# Copyright (c) 2019-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""EEPROM management for PyFdti"""

# pylint: disable=wrong-import-position
# pylint: disable=import-error

import sys
from binascii import hexlify, unhexlify
from collections import OrderedDict, namedtuple
from configparser import ConfigParser
from enum import IntEnum, IntFlag
from logging import getLogger
from random import randint
from re import match
from struct import calcsize as scalc, pack as spack, unpack as sunpack
from typing import BinaryIO, List, Optional, Set, TextIO, Union, Tuple
from usb.core import Device as UsbDevice
from .ftdi import Ftdi, FtdiError
from .misc import classproperty, to_bool, to_int


class FtdiEepromError(FtdiError):
    """FTDI EEPROM error."""


class Hex2Int(int):
    """Hexa representation of a byte."""
    def __str__(self):
        return f'0x{int(self):02x}'


class Hex4Int(int):
    """Hexa representation of a half-word."""
    def __str__(self):
        return f'0x{int(self):04x}'


class FtdiEeprom:
    """FTDI EEPROM management
    """

    _PROPS = namedtuple('PROPS', 'size user dynoff chipoff')
    """Properties for each FTDI device release.

       * size is the size in bytes of the EEPROM storage area
       * user is the size in bytes of the user storage area, if any/supported
       * dynoff is the offset in EEPROM of the first bytes to store strings
       * chipoff is the offset in EEPROM of the EEPROM chip type
    """

    _PROPERTIES = {
        0x0200: _PROPS(0, None, 0, None),        # FT232AM
        0x0400: _PROPS(256, 0x14, 0x94, None),   # FT232BM
        0x0500: _PROPS(256, 0x16, 0x96, 0x14),   # FT2232D
        0x0600: _PROPS(128, None, 0x18, None),   # FT232R
        0x0700: _PROPS(256, 0x1A, 0x9A, 0x18),   # FT2232H
        0x0800: _PROPS(256, 0x1A, 0x9A, 0x18),   # FT4232H
        0x0900: _PROPS(256, 0x1A, 0xA0, 0x1e),   # FT232H
        0x1000: _PROPS(1024, 0x1A, 0xA0, None),  # FT230X/FT231X/FT234X
        0x3600: _PROPS(256, 0x1A, 0x9A, 0x18),   # FT4232HA
    }
    """EEPROM properties."""

    CBUS = IntEnum('CBus',
                   'TXDEN PWREN TXLED RXLED TXRXLED SLEEP CLK48 CLK24 CLK12 '
                   'CLK6 GPIO BB_WR BB_RD', start=0)
    """Alternate features for legacy FT232R devices."""

    CBUSH = IntEnum('CBusH',
                    'TRISTATE TXLED RXLED TXRXLED PWREN SLEEP DRIVE0 DRIVE1 '
                    'GPIO TXDEN CLK30 CLK15 CLK7_5', start=0)
    """Alternate features for FT232H/FT2232H/FT4232H devices."""

    CBUSX = IntEnum('CBusX',
                    'TRISTATE TXLED RXLED TXRXLED PWREN SLEEP DRIVE0 DRIVE1 '
                    'GPIO TXDEN CLK24 CLK12 CLK6 BAT_DETECT BAT_NDETECT '
                    'I2C_TXE I2C_RXF VBUS_SENSE BB_WR BB_RD TIMESTAMP AWAKE',
                    start=0)
    """Alternate features for FT230X devices."""

    UART_BITS = IntFlag('UartBits', 'TXD RXD RTS CTS DTR DSR DCD RI')
    """Inversion flags for FT232R and FT-X devices."""

    CHANNEL = IntFlag('Channel', 'FIFO OPTO CPU FT128 RS485')
    """Alternate port mode."""

    DRIVE = IntFlag('Drive',
                    'LOW HIGH SLOW_SLEW SCHMITT _10 _20 _40 PWRSAVE_DIS')
    """Driver options for I/O pins."""

    CFG1 = IntFlag('Cfg1', 'CLK_IDLE_STATE DATA_LSB FLOW_CONTROL _08 '
                           'HIGH_CURRENTDRIVE _20 _40 SUSPEND_DBUS7')
    """Configuration bits stored @ 0x01."""

    VAR_STRINGS = ('manufacturer', 'product', 'serial')
    """EEPROM strings with variable length."""

    def __init__(self):
        self.log = getLogger('pyftdi.eeprom')
        self._ftdi = Ftdi()
        self._eeprom = bytearray()
        self._size = 0
        self._dev_ver = 0
        self._valid = False
        self._config = OrderedDict()
        self._dirty = set()
        self._modified = False
        self._chip: Optional[int] = None
        self._mirror = False
        self._test_mode = False

    def __getattr__(self, name):
        if name in self._config:
            return self._config[name]
        raise AttributeError(f'No such attribute: {name}')

    @classproperty
    def eeprom_sizes(cls) -> List[int]:
        # pylint: disable=no-self-argument
        """Return a list of supported EEPROM sizes.

           :return: the supported EEPROM sizes
        """
        return sorted({p.size for p in cls._PROPERTIES.values() if p.size})

    def open(self, device: Union[str, UsbDevice],
             ignore: bool = False, size: Optional[int] = None,
             model: Optional[str] = None) -> None:
        """Open a new connection to the FTDI USB device.

           :param device: the device URL or a USB device instance.
           :param ignore: whether to ignore existing content
           :param size: a custom EEPROM size
           :param model: the EEPROM model used to specify a custom size
        """
        if self._ftdi.is_connected:
            raise FtdiError('Already open')
        if isinstance(device, str):
            self._ftdi.open_from_url(device)
        else:
            self._ftdi.open_from_device(device)
        if model and not size:
            # 93xxx46/56/66
            mmo = match(r'(?i)^93[a-z]*([456])6.*$', model)
            if not mmo:
                raise ValueError(f'Unknown EEPROM device: {model}')
            mmul = int(mmo.group(1))
            size = 128 << (mmul - 4)
        if size:
            if size not in self.eeprom_sizes:
                raise ValueError(f'Unsupported EEPROM size: {size}')
            self._size = min(size, 256)
        if not ignore:
            self._eeprom = self._read_eeprom()
            if self._valid:
                self._decode_eeprom()

    def close(self) -> None:
        """Close the current connection to the FTDI USB device,
        """
        if self._ftdi.is_connected:
            self._ftdi.close()
            self._eeprom = bytearray()
            self._dev_ver = 0
            self._config.clear()

    def connect(self, ftdi: Ftdi, ignore: bool = False) -> None:
        """Connect a FTDI EEPROM to an existing Ftdi instance.

           :param ftdi: the Ftdi instance to use
           :param ignore: whether to ignore existing content
        """
        self._ftdi = ftdi
        self._eeprom = bytearray()
        self._dev_ver = 0
        self._valid = False
        self._config = OrderedDict()
        self._dirty = set()
        if not ignore:
            self._eeprom = self._read_eeprom()
            if self._valid:
                self._decode_eeprom()
            self._decode_eeprom()

    @property
    def device_version(self) -> int:
        """Report the version of the FTDI device.

           :return: the release
        """
        if not self._dev_ver:
            if not self._ftdi.is_connected:
                raise FtdiError('Not connected')
            self._dev_ver = self._ftdi.device_version
        return self._dev_ver

    @property
    def size(self) -> int:
        """Report the EEPROM size.

           Use the most common (default) EEPROM size of the size is not yet
           known.

           :return: the size in bytes
        """
        if not self._size:
            self._size = self.default_size
        return self._size

    @property
    def default_size(self) -> int:
        """Report the default EEPROM size based on the FTDI type.

           The physical EEPROM size may be greater or lower, depending on the
           actual connected EEPROM device.

           :return: the size in bytes
        """
        if self._chip == 0x46:
            return 0x80  # 93C46
        if self._chip == 0x56:
            return 0x100  # 93C56
        if self._chip == 0x66:
            return 0x100  # 93C66 (512 bytes, only 256 are used)
        try:
            eeprom_size = self._PROPERTIES[self.device_version].size
        except (AttributeError, KeyError) as exc:
            raise FtdiError('No EEPROM') from exc
        return eeprom_size

    @property
    def storage_size(self) -> int:
        """Report the number of EEPROM bytes that can be used for configuration
            storage. The physical EEPROM size may be greater

            :return: the number of bytes in the eeprom that will be used for
                configuration storage
        """
        try:
            eeprom_storage_size = self.size
            if self.is_mirroring_enabled:
                eeprom_storage_size = self.mirror_sector
        except FtdiError as exc:
            raise exc
        return eeprom_storage_size

    @property
    def data(self) -> bytes:
        """Returns the content of the EEPROM.

           :return: the content as bytes.
        """
        self._sync_eeprom()
        return bytes(self._eeprom)

    @property
    def properties(self) -> Set[str]:
        """Returns the supported properties for the current device.

           :return: the supported properies.
        """
        props = set(self._config.keys())
        props -= set(self.VAR_STRINGS)
        return props

    @property
    def is_empty(self) -> bool:
        """Reports whether the EEPROM has been erased, or no EEPROM is
           connected to the FTDI EEPROM port.

           :return: True if no content is detected
        """
        if len(self._eeprom) != self.size:
            return False
        for byte in self._eeprom:
            if byte != 0xFF:
                return False
        return True

    @property
    def cbus_pins(self) -> List[int]:
        """Return the list of CBUS pins configured as GPIO, if any

           :return: list of CBUS pins
        """
        pins = [pin for pin in range(0, 10)
                if self._config.get(f'cbus_func_{pin}', '') == 'GPIO']
        return pins

    @property
    def cbus_mask(self) -> int:
        """Return the bitmask of CBUS pins configured as GPIO.

           The bitmap contains four bits, ordered in natural order.

           :return: CBUS mask
        """
        if self.device_version == 0x900:  # FT232H
            cbus = [5, 6, 8, 9]
        else:
            cbus = list(range(4))
        mask = 0
        for bix, pin in enumerate(cbus):
            if self._config.get(f'cbus_func_{pin}', '') == 'GPIO':
                mask |= 1 << bix
        return mask

    @property
    def has_mirroring(self) -> bool:
        """Report whether the device supports EEPROM content duplication
           across its two sectors.

           :return: True if the device support mirorring
        """
        return (self._PROPERTIES[self.device_version].user and
                self._ftdi.device_version != 0x1000)

    @property
    def mirror_sector(self) -> int:
        """Report start address of the mirror sector in the EEPROM.
           This is only valid if the FTDI is capable of mirroring EEPROM data.

           :return: the start address
        """
        if self.has_mirroring:
            return self.size // 2
        raise FtdiError('EEPROM does not support mirroring')

    @property
    def is_mirroring_enabled(self) -> bool:
        """Check if EEPROM mirroring is currently enabled for this EEPROM.
            See enable_mirroring for more details on EEPROM mirroring
            functionality
        """
        return self.has_mirroring and self._mirror

    def enable_mirroring(self, enable: bool) -> None:
        """Enable EEPROM write mirroring. When enabled, this divides the EEPROM
           into 2 sectors and mirrors configuration data between them.

           For example on a 256 byte EEPROM, two 128 byte 'sectors' will be
           used to store identical data. Configuration properties/strings will
           be writen to both of these sectors. For some devices (like the
           4232H), this makes the PyFtdi EEPROM functionally similar to
           FT_PROG.

           Note: Data will only be mirrored if the has_mirroring property
           returns true (after establishing a connection to the ftdi)

           :param enable: enable or disable EEPROM mirroring
        """
        self._mirror = enable

    def save_config(self, file: TextIO) -> None:
        """Save the EEPROM content as an INI stream.

           :param file: output stream
        """
        self._sync_eeprom()
        cfg = ConfigParser()
        cfg.add_section('values')
        for name, value in self._config.items():
            val = str(value)
            if isinstance(value, bool):
                val = val.lower()
            cfg.set('values', name, val)
        cfg.add_section('raw')
        length = 16
        for i in range(0, len(self._eeprom), length):
            chunk = self._eeprom[i:i+length]
            hexa = hexlify(chunk).decode()
            cfg.set('raw', f'@{i:02x}', hexa)
        cfg.write(file)

    def load_config(self, file: TextIO, section: Optional[str] = None) -> None:
        """Load the EEPROM content from an INI stream.

           The ``section`` argument selects which section(s) to load:

           * ``raw`` only loads the raw data (hexabytes) from a previous dump
           * ``values`` only loads the values section, that is the human
             readable configuration.
           * ``all``, which is the default section selection, load the raw
             section, then overwrite part of it with any configuration value
             from the ``values`` section. This provides a handy way to use an
             existing dump from a valid EEPROM content, while customizing some
             parameters, such as the serial number.

           :param file: input stream
           :paran section: which section to load from the ini file
        """
        self._sync_eeprom()
        cfg = ConfigParser()
        cfg.read_file(file)
        loaded = False
        sections = cfg.sections()
        if section not in ('all', None) and section not in sections:
            raise FtdiEepromError(f'No such configuration section {section}')
        sect = 'raw'
        if sect in sections and section in (None, 'all', sect):
            if not cfg.has_section(sect):
                raise FtdiEepromError(f"No '{sect}' section in INI file")
            options = cfg.options(sect)
            try:
                for opt in options:
                    if not opt.startswith('@'):
                        raise ValueError()
                    address = int(opt[1:], 16)
                    hexval = cfg.get(sect, opt).strip()
                    buf = unhexlify(hexval)
                    self._eeprom[address:address+len(buf)] = buf
            except IndexError as exc:
                raise ValueError(f"Invalid address in '{sect}' "
                                 f"section") from exc
            except ValueError as exc:
                raise ValueError(f"Invalid line in '{sect}' section") from exc
            self._compute_crc(self._eeprom, True)
            if not self._valid:
                raise ValueError('Loaded RAW section is invalid (CRC mismatch')
            loaded = True
        sect = 'values'
        vmap = {
            'manufacturer': 'manufacturer_name',
            'product': 'product_name',
            'serial': 'serial_number'
        }
        if sect in sections and section in (None, 'all', sect):
            if not cfg.has_section(sect):
                raise FtdiEepromError(f"No '{sect}' section in INI file")
            options = cfg.options(sect)
            for opt in options:
                value = cfg.get(sect, opt).strip()
                if opt in vmap:
                    func = getattr(self, f'set_{vmap[opt]}')
                    func(value)
                else:
                    self.log.debug('Assigning opt %s = %s', opt, value)
                    try:
                        self.set_property(opt, value)
                    except (TypeError, ValueError, NotImplementedError) as exc:
                        self.log.warning("Ignoring setting '%s': %s", opt, exc)
            loaded = True
        if not loaded:
            raise ValueError(f'Invalid section: {section}')
        self._sync_eeprom()

    def set_serial_number(self, serial: str) -> None:
        """Define a new serial number."""
        self._validate_string(serial)
        self._update_var_string('serial', serial)
        self.set_property('has_serial', True)

    def set_manufacturer_name(self, manufacturer: str) -> None:
        """Define a new manufacturer string."""
        self._validate_string(manufacturer)
        self._update_var_string('manufacturer', manufacturer)

    def set_product_name(self, product: str) -> None:
        """Define a new product name."""
        self._validate_string(product)
        self._update_var_string('product', product)

    def set_property(self, name: str, value: Union[str, int, bool],
                     out: Optional[TextIO] = None) -> None:
        """Change the value of a stored property.

           :see: :py:meth:`properties` for a list of valid property names.
                 Note that for now, only a small subset of properties can be
                 changed.
           :param name: the property to change
           :param value: the new value (supported values depend on property)
           :param out: optional output stream to report hints
        """
        mobj = match(r'cbus_func_(\d)', name)
        if mobj:
            if not isinstance(value, str):
                raise ValueError("'{name}' should be specified as a string")
            self._set_cbus_func(int(mobj.group(1)), value, out)
            self._dirty.add(name)
            return
        mobj = match(r'([abcd])bus_(drive|slow_slew|schmitt)', name)
        if mobj:
            self._set_bus_control(mobj.group(1), mobj.group(2), value, out)
            self._dirty.add(name)
            return
        mobj = match(r'group_(\d)_(drive|schmitt|slow_slew)', name)
        if mobj:
            self._set_group(int(mobj.group(1)), mobj.group(2), value, out)
            self._dirty.add(name)
            return
        mobj = match(r'channel_([abcd])_type', name)
        if mobj:
            chn = mobj.group(1)
            if value == 'UART':
                val = 0
            else:
                val = self.CHANNEL[value]
            if self.device_version == 0x0700 and chn in 'ab':
                # FT2232H
                idx = 0x00 if chn == 'a' else 0x01
                mask = 0x07
            elif self.device_version == 0x0800:
                # FT4232H
                idx = 0x0b
                mask = 1 << {'a': 4, 'b': 5, 'c': 6, 'd': 7}.get(chn)
                val = mask if val > 0 else 0
            elif self.device_version == 0x0900 and chn == 'a':
                # FT232H
                idx = 0x00
                mask = 0x0F
            else:
                raise ValueError(
                    f"Option '{name}' not supported by the device")
            if val & ~mask:
                raise ValueError(
                    f"Unsupported value for setting '{name}': {val}")
            self._eeprom[idx] &= ~mask
            self._eeprom[idx] |= val
            if self.is_mirroring_enabled:
                idx2 = self.mirror_sector + idx
                self._eeprom[idx2] &= ~mask
                self._eeprom[idx2] |= val
            self._dirty.add(name)
            return
        mobj = match(r'channel_([abcd])_driver', name)
        if mobj:
            chn = mobj.group(1)
            if value == 'VCP':
                val = 1
            elif value == 'D2XX':
                val = 0
            else:
                raise ValueError(
                    f"Invalid value '{value} for '{name}'")
            if self.device_version == 0x0700 and chn in 'ab':
                # FT2232H
                idx = 0x00 if chn == 'a' else 0x01
                mask = 1 << 3
            elif self.device_version == 0x0800:
                # FT4232H
                idx = {'a': 0, 'b': 1, 'c': 0, 'd': 1}.get(chn)
                mask = 1 << {'a': 3, 'b': 3, 'c': 7, 'd': 7}.get(chn)
            elif self.device_version == 0x0900 and chn == 'a':
                # FT232H
                idx = 0x00
                mask = 1 << 4
            else:
                raise ValueError(
                    f"Option '{name}' not supported by the device")
            self._eeprom[idx] &= ~mask
            if val:
                self._eeprom[idx] |= mask
            if self.is_mirroring_enabled:
                idx2 = self.mirror_sector + idx
                self._eeprom[idx2] &= ~mask
                if val:
                    self._eeprom[idx2] |= mask
            self._dirty.add(name)
            return
        confs = {
            'remote_wakeup': (0, 5),
            'self_powered': (0, 6),
            'in_isochronous': (2, 0),
            'out_isochronous': (2, 1),
            'suspend_pull_down': (2, 2),
            'has_serial': (2, 3),
        }
        hwords = {
            'vendor_id': 0x02,
            'product_id': 0x04,
            'type': 0x06,
        }
        if self.device_version in (0x0400, 0x0500):
            # Type BM and 2232C/D use 0xc to encode the USB version to expose
            # H device use this location to encode bus/group properties
            hwords['usb_version'] = 0x0c
            confs['use_usb_version'] = (2, 4)
        if name in hwords:
            val = to_int(value)
            if not 0 <= val <= 0xFFFF:
                raise ValueError(f'Invalid value for {name}')
            offset = hwords[name]
            self._eeprom[offset:offset+2] = spack('<H', val)
            if self.is_mirroring_enabled:
                # duplicate in 'sector 2'
                offset2 = self.mirror_sector + offset
                self._eeprom[offset2:offset2+2] = spack('<H', val)
            self._dirty.add(name)
            return
        if name in confs:
            val = to_bool(value, permissive=False, allow_int=True)
            offset, bit = confs[name]
            mask = 1 << bit
            idx = 0x08 + offset
            if val:
                self._eeprom[idx] |= mask
                if self.is_mirroring_enabled:
                    # duplicate in 'sector 2'
                    idx2 = self.mirror_sector + idx
                    self._eeprom[idx2] |= mask
            else:
                self._eeprom[idx] &= ~mask
                if self.is_mirroring_enabled:
                    # duplicate in 'sector 2'
                    idx2 = self.mirror_sector + idx
                    self._eeprom[idx2] &= ~mask
            self._dirty.add(name)
            return
        if name == 'power_max':
            val = to_int(value) >> 1
            idx = 0x09
            self._eeprom[idx] = val
            if self.is_mirroring_enabled:
                # duplicate in 'sector 2'
                idx2 = self.mirror_sector + idx
                self._eeprom[idx2] = val
            self._dirty.add(name)
            return
        if name.startswith('invert_'):
            if self.device_version not in (0x600, 0x1000):
                raise ValueError('UART control line inversion not available '
                                 'with this device')
            self._set_invert(name[len('invert_'):], value, out)
            self._dirty.add(name)
            return
        if name == 'chip':
            val = to_int(value)
            idx = self._PROPERTIES[self.device_version].chipoff
            if idx is None:
                raise ValueError(
                    f"Setting '{name}' is not supported by the chip")
            self._eeprom[idx] = val
            if self.is_mirroring_enabled:
                idx2 = self.mirror_sector + idx
                self._eeprom[idx2] = val
            self._dirty.add(name)
            return
        if name == 'suspend_dbus7':
            val = to_bool(value, permissive=False, allow_int=True)
            if self.device_version == 0x0700:
                # FT2232H
                idx = 0x01
                mask = self.CFG1.SUSPEND_DBUS7.value
                self._eeprom[idx] &= ~mask
                if val:
                    self._eeprom[idx] |= mask
                if self.is_mirroring_enabled:
                    idx2 = self.mirror_sector + idx
                    self._eeprom[idx2] &= ~mask
                    if val:
                        self._eeprom[idx2] |= mask
            else:
                raise ValueError(
                    f"Setting '{name}' is not supported by the chip")
            self._dirty.add(name)
            return
        if name in self.properties:
            if name not in self._config:
                raise NotImplementedError('Change is not supported')
            curval = self._config[name]
            try:
                curtype = type(curval)
                value = curtype(value)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Cannot be converted to the proper type "
                                 f"'{curtype}'") from exc
            if value != curval:
                raise NotImplementedError('Not yet supported')
            # no-op change is silently ignored
            return
        raise ValueError(f'Unknown property: {name}')

    def erase(self, erase_byte: Optional[int] = 0xFF) -> None:
        """Erase the whole EEPROM.

            :param erase_byte: Optional erase byte to use. Default to 0xFF
        """
        self._eeprom = bytearray([erase_byte] * self.size)
        self._config.clear()
        self._dirty.add('eeprom')

    def initialize(self) -> None:
        """Initialize the EEPROM with some default sensible values.
        """
        dev_ver = self.device_version
        dev_name = Ftdi.DEVICE_NAMES[dev_ver]
        vid = Ftdi.FTDI_VENDOR
        pid = Ftdi.PRODUCT_IDS[vid][dev_name]
        self.set_manufacturer_name('FTDI')
        self.set_product_name(dev_name.upper())
        sernum = ''.join([chr(randint(ord('A'), ord('Z'))) for _ in range(5)])
        self.set_serial_number(f'FT{randint(0, 9)}{sernum}')
        self.set_property('vendor_id', vid)
        self.set_property('product_id', pid)
        self.set_property('type', dev_ver)
        self.set_property('power_max', 150)
        self._sync_eeprom()

    def sync(self) -> None:
        """Force re-evaluation of configuration after some changes.

           This API is not useful for regular usage, but might help for testing
           when the EEPROM does not go through a full save/load cycle
        """
        self._sync_eeprom()

    def dump_config(self, file: Optional[BinaryIO] = None) -> None:
        """Dump the configuration to a file.

           :param file: the output file, default to stdout
        """
        if self._dirty:
            self._decode_eeprom()
        for name, value in self._config.items():
            print(f'{name}: {value}', file=file or sys.stdout)

    def commit(self, dry_run: bool = True, no_crc: bool = False) -> bool:
        """Commit any changes to the EEPROM.

           :param dry_run: log what should be written, do not actually change
                  the EEPROM content
           :param no_crc: do not compute EEPROM CRC. This should only be used
            to perform a full erasure of the EEPROM, as an attempt to recover
            from a corrupted config.

           :return: True if some changes have been committed to the EEPROM
        """
        self._sync_eeprom(no_crc)
        if not self._modified:
            self.log.warning('No change to commit, EEPROM not modified')
            return False
        self._ftdi.overwrite_eeprom(self._eeprom, dry_run=dry_run)
        if not dry_run:
            eeprom = self._read_eeprom()
            if eeprom != self._eeprom:
                pos = 0
                for pos, (old, new) in enumerate(zip(self._eeprom, eeprom)):
                    if old != new:
                        break
                pos &= ~0x1
                raise FtdiEepromError(f'Write to EEPROM failed @ 0x{pos:02x}')
            self._modified = False
        return dry_run

    def reset_device(self):
        """Execute a USB device reset."""
        self._ftdi.reset(usb_reset=True)

    def set_test_mode(self, enable: bool):
        """Enable test mode (silence some warnings)."""
        self._test_mode = enable

    @classmethod
    def _validate_string(cls, string):
        for invchr in ':/':
            # do not accept characters which are interpreted as URL seperators
            if invchr in string:
                raise ValueError(f"Invalid character '{invchr}' in string")

    def _update_var_string(self, name: str, value: str) -> None:
        if name not in self.VAR_STRINGS:
            raise ValueError(f'{name} is not a variable string')
        try:
            if value == self._config[name]:
                return
        except KeyError:
            # not yet defined
            pass
        self._config[name] = value
        self._dirty.add(name)

    def _generate_var_strings(self, fill=True) -> None:
        """
            :param fill: fill the remainder of the space after the var strings
                with 0s
        """
        stream = bytearray()
        dynpos = self._PROPERTIES[self.device_version].dynoff
        if dynpos > self._size:
            # if a custom, small EEPROM device is used
            dynpos = 0x40
        data_pos = dynpos
        tbl_pos = 0x0e
        if self.is_mirroring_enabled:
            # start of var-strings in sector 1 (used for mirrored config)
            s1_vstr_start = data_pos - self.mirror_sector
            tbl_sector2_pos = self.mirror_sector + tbl_pos
        for name in self.VAR_STRINGS:
            try:
                ustr = self._config[name].encode('utf-16le')
            except KeyError:
                ustr = ''
            length = len(ustr)+2
            stream.append(length)
            stream.append(0x03)  # string descriptor
            stream.extend(ustr)
            self._eeprom[tbl_pos] = data_pos | 0x80
            tbl_pos += 1
            if self.is_mirroring_enabled:
                self._eeprom[tbl_sector2_pos] = data_pos
                tbl_sector2_pos += 1
            self._eeprom[tbl_pos] = length
            tbl_pos += 1
            if self.is_mirroring_enabled:
                self._eeprom[tbl_sector2_pos] = length
                tbl_sector2_pos += 1
            data_pos += length
        if self.is_mirroring_enabled:
            self._eeprom[s1_vstr_start:s1_vstr_start+len(stream)] = stream
        self._eeprom[dynpos:dynpos+len(stream)] = stream
        mtp = self._ftdi.device_version == 0x1000
        crc_pos = 0x100 if mtp else self._size
        rem = crc_pos - (dynpos + len(stream))
        if rem < 0:
            oversize = (-rem + 2) // 2
            raise FtdiEepromError(f'Cannot fit strings into EEPROM, '
                                  f'{oversize} oversize characters')
        if fill:
            self._eeprom[dynpos+len(stream):crc_pos] = bytes(rem)
            if self.is_mirroring_enabled:
                crc_s1_pos = self.mirror_sector
                self._eeprom[s1_vstr_start+len(stream):crc_s1_pos] = bytes(rem)

    def _sync_eeprom(self, no_crc: bool = False):
        if not self._dirty:
            self.log.debug('No change detected for EEPROM content')
            return
        if not no_crc:
            if any(x in self._dirty for x in self.VAR_STRINGS):
                self._generate_var_strings()
                for varstr in self.VAR_STRINGS:
                    self._dirty.discard(varstr)
            self._update_crc()
            self._decode_eeprom()
        self._dirty.clear()
        self._modified = True
        self.log.debug('EEPROM content regenerated (not yet committed)')

    def _compute_crc(self, eeprom: Union[bytes, bytearray], check=False):
        mtp = self._ftdi.device_version == 0x1000
        crc_pos = 0x100 if mtp else len(eeprom)
        crc_size = scalc('<H')
        if not check:
            # check mode: add CRC itself, so that result should be zero
            crc_pos -= crc_size
        if self.is_mirroring_enabled:
            mirror_s1_crc_pos = self.mirror_sector
            if not check:
                mirror_s1_crc_pos -= crc_size
            # if mirroring, only calculate the crc for the first sector/half
            #   of the eeprom. Data (including this crc) are duplicated in
            #   the second sector/half
            crc = self._ftdi.calc_eeprom_checksum(eeprom[:mirror_s1_crc_pos])
        else:
            crc = self._ftdi.calc_eeprom_checksum(eeprom[:crc_pos])
        if check:
            self._valid = not bool(crc)
            if not self._valid:
                self.log.debug('CRC is now 0x%04x', crc)
            else:
                self.log.debug('CRC OK')
        return crc, crc_pos, crc_size

    def _update_crc(self):
        crc, crc_pos, crc_size = self._compute_crc(
            self._eeprom, False)
        self._eeprom[crc_pos:crc_pos+crc_size] = spack('<H', crc)
        if self.is_mirroring_enabled:
            # if mirroring calculate where the CRC will start in first sector
            crc_s1_start = self.mirror_sector - crc_size
            self._eeprom[crc_s1_start:crc_s1_start+crc_size] = spack('<H', crc)

    def _compute_size(self, eeprom: Union[bytes, bytearray]) \
            -> Tuple[int, bool]:
        """
            :return: Tuple of:
                - int of usable size of the eeprom
                - bool of whether eeprom mirroring was detected or not
        """
        if self._ftdi.is_eeprom_internal:
            return self._ftdi.max_eeprom_size, False
        if all(x == 0xFF for x in eeprom):
            # erased EEPROM, size is unknown
            return self._ftdi.max_eeprom_size, False
        if eeprom[0:0x80] == eeprom[0x80:0x100]:
            return 0x80, True
        if eeprom[0:0x40] == eeprom[0x40:0x80]:
            return 0x40, True
        return 0x100, False

    def _read_eeprom(self) -> bytes:
        buf = self._ftdi.read_eeprom(0, eeprom_size=self.size)
        eeprom = bytearray(buf)
        size, mirror_detected = self._compute_size(eeprom)
        if size < len(eeprom):
            eeprom = eeprom[:size]
        crc = self._compute_crc(eeprom, True)[0]
        if crc:
            if self.is_empty:
                self.log.info('No EEPROM or EEPROM erased')
            else:
                self.log.error('Invalid CRC or EEPROM content')
        if not self.is_empty and mirror_detected:
            self.log.info('Detected a mirrored eeprom. '
                          'Enabling mirrored writing')
            self._mirror = True
        return eeprom

    def _decode_eeprom(self):
        cfg = self._config
        cfg.clear()
        chipoff = self._PROPERTIES[self.device_version].chipoff
        if chipoff is not None:
            self._chip = Hex2Int(self._eeprom[chipoff])
            cfg['chip'] = self._chip
        cfg['vendor_id'] = Hex4Int(sunpack('<H', self._eeprom[0x02:0x04])[0])
        cfg['product_id'] = Hex4Int(sunpack('<H', self._eeprom[0x04:0x06])[0])
        cfg['type'] = Hex4Int(sunpack('<H', self._eeprom[0x06:0x08])[0])
        power_supply, power_max, conf = sunpack('<3B', self._eeprom[0x08:0x0b])
        cfg['self_powered'] = bool(power_supply & (1 << 6))
        cfg['remote_wakeup'] = bool(power_supply & (1 << 5))
        cfg['power_max'] = power_max << 1
        cfg['has_serial'] = bool(conf & (1 << 3))
        cfg['suspend_pull_down'] = bool(conf & (1 << 2))
        cfg['out_isochronous'] = bool(conf & (1 << 1))
        cfg['in_isochronous'] = bool(conf & (1 << 0))
        cfg['manufacturer'] = self._decode_string(0x0e)
        cfg['product'] = self._decode_string(0x10)
        cfg['serial'] = self._decode_string(0x12)
        if self.device_version in (0x0400, 0x0500):
            cfg['use_usb_version'] = bool(conf & (1 << 3))
            if cfg['use_usb_version']:
                cfg['usb_version'] = \
                    Hex4Int(sunpack('<H', self._eeprom[0x0c:0x0e])[0])
        if cfg['type'] == 0xffff:
            if not self._test_mode:
                self.log.warning('EEPROM type is erased')
            return
        name = None
        try:
            type_ = cfg['type']
            if type_ == 0:
                type_ = self.device_version
            name = Ftdi.DEVICE_NAMES[type_].replace('-', '')
            if name.startswith('ft'):
                name = name[2:]
            func = getattr(self, f'_decode_{name}')
        except (KeyError, AttributeError):
            self.log.warning('No EEPROM decoder for device %s', name or '?')
        else:
            func()

    def _decode_string(self, offset):
        str_offset, str_size = sunpack('<BB', self._eeprom[offset:offset+2])
        if str_size:
            str_offset &= self.size - 1
            str_size -= scalc('<H')
            str_offset += scalc('<H')
            manufacturer = self._eeprom[str_offset:str_offset+str_size]
            return manufacturer.decode('utf16', errors='ignore')
        return ''

    def _set_cbus_func(self, cpin: int, value: str,
                       out: Optional[TextIO]) -> None:
        cmap = {0x600: (self.CBUS, 5, 0x14, 4),    # FT232R
                0x900: (self.CBUSH, 10, 0x18, 4),  # FT232H
                0x1000: (self.CBUSX, 4, 0x1A, 8)}  # FT230X/FT231X/FT234X
        try:
            cbus, count, offset, width = cmap[self.device_version]
        except KeyError as exc:
            raise ValueError('This property is not supported on this '
                             'device') from exc
        pin_filter = getattr(self,
                             f'_filter_cbus_func_x{self.device_version:x}',
                             None)
        if value == '?' and out:
            items = {item.name for item in cbus}
            if pin_filter:
                items = {val for val in items if pin_filter(cpin, val)}
            print(', '.join(sorted(items)) if items else '(none)', file=out)
            return
        if not 0 <= cpin < count:
            raise ValueError(f"Unsupported CBUS pin '{cpin}'")
        try:
            code = cbus[value.upper()].value
        except KeyError as exc:
            raise ValueError(f"CBUS pin '{cpin}' does not have function "
                             f"{value}'") from exc
        if pin_filter and not pin_filter(cpin, value.upper()):
            raise ValueError(f"Unsupported CBUS function '{value}' for pin "
                             f"'{cpin}'")
        addr = offset + (cpin*width)//8
        if width == 4:
            bitoff = 4 if cpin & 0x1 else 0
            mask = 0x0F << bitoff
        else:
            bitoff = 0
            mask = 0xFF
        old = self._eeprom[addr]
        self._eeprom[addr] &= ~mask
        self._eeprom[addr] |= code << bitoff
        self.log.debug('Cpin %d, addr 0x%02x, value 0x%02x->0x%02x',
                       cpin, addr, old, self._eeprom[addr])

    @classmethod
    def _filter_cbus_func_x900(cls, cpin: int, value: str):
        if cpin == 7:
            # nothing can be assigned to ACBUS7
            return False
        if value in 'TRISTATE TXLED RXLED TXRXLED PWREN SLEEP DRIVE0'.split():
            # any pin can be assigned these functions
            return True
        if cpin in (5, 6, 8, 9):
            # any function can be assigned to ACBUS5, ACBUS6, ACBUS8, ACBUS9
            return True
        if cpin == 0:
            return value != 'GPIO'
        return False

    @classmethod
    def _filter_cbus_func_x600(cls, cpin: int, value: str):
        if value == 'BB_WR':
            # this signal is only available on CBUS0, CBUS1
            return cpin < 2
        return True

    def _set_bus_control(self, bus: str, control: str,
                         value: Union[str, int, bool],
                         out: Optional[TextIO]) -> None:
        if self.device_version == 0x1000:
            self._set_bus_control_230x(bus, control, value, out)
            return
        # for now, only support FT-X devices
        raise ValueError('Bus control not implemented for this device')

    def _set_group(self, group: int, control: str,
                   value: Union[str, int, bool], out: Optional[TextIO]) \
            -> None:
        if self.device_version in (0x0700, 0x0800, 0x0900, 0x3600):
            self._set_group_x232h(group, control, value, out)
            return
        raise ValueError('Group not implemented for this device')

    def _set_bus_control_230x(self, bus: str, control: str,
                              value: Union[str, int, bool],
                              out: Optional[TextIO]) -> None:
        if bus not in 'cd':
            raise ValueError(f'Invalid bus: {bus}')
        self._set_bus_xprop(0x0c, bus == 'c', control, value, out)

    def _set_group_x232h(self, group: int, control: str, value: str,
                         out: Optional[TextIO]) -> None:
        # 2232H/4232H/4232HA
        if self.device_version in (0x0700, 0x800, 0x3600):
            offset = 0x0c + group//2
            nibble = group & 1
        else:  # 232H
            offset = 0x0c + group
            nibble = 0
        self._set_bus_xprop(offset, nibble, control, value, out)

    def _set_bus_xprop(self, offset: int, high_nibble: bool, control: str,
                       value: Union[str, int, bool], out: Optional[TextIO]) \
            -> None:
        try:
            if control == 'drive':
                candidates = (4, 8, 12, 16)
                if value == '?' and out:
                    print(', '.join([str(v) for v in candidates]), file=out)
                    return
                value = int(value)
                if value not in candidates:
                    raise ValueError(f'Invalid drive current: {value} mA')
                value //= 4
                value -= 1
            elif control in ('slow_slew', 'schmitt'):
                if value == '?' and out:
                    print('off, on', file=out)
                    return
                value = int(to_bool(value))
            else:
                raise ValueError(f'Unsupported control: {control}')
        except (ValueError, TypeError) as exc:
            raise ValueError(f'Invalid {control} value: {value}') from exc
        config = self._eeprom[offset]
        if not high_nibble:
            conf = config & 0x0F
            config &= 0xF0
            cshift = 0
        else:
            conf = config >> 4
            config &= 0x0F
            cshift = 4
        if control == 'drive':
            conf &= 0b1100
            conf |= value
        elif control == 'slow_slew':
            conf &= 0b1011
            conf |= value << 2
        elif control == 'schmitt':
            conf &= 0b0111
            conf |= value << 3
        else:
            raise RuntimeError('Internal error')
        config |= conf << cshift
        self._eeprom[offset] = config

    def _set_invert(self, name, value, out):
        if value == '?' and out:
            print('off, on', file=out)
            return
        if name.upper() not in self.UART_BITS.__members__:
            raise ValueError(f'Unknown property: {name}')
        value = to_bool(value, permissive=False)
        code = getattr(self.UART_BITS, name.upper())
        invert = self._eeprom[0x0B]
        if value:
            invert |= code
        else:
            invert &= ~code
        self._eeprom[0x0B] = invert

    def _decode_x(self):
        # FT-X series
        cfg = self._config
        misc, = sunpack('<H', self._eeprom[0x00:0x02])
        cfg['channel_a_driver'] = 'VCP' if misc & (1 << 7) else 'D2XX'
        for bit in self.UART_BITS:
            value = self._eeprom[0x0B]
            cfg[f'invert_{self.UART_BITS(bit).name}'] = bool(value & bit)
        max_drive = self.DRIVE.LOW.value | self.DRIVE.HIGH.value
        value = self._eeprom[0x0c]
        for grp in range(2):
            conf = value & 0xF
            bus = 'c' if grp else 'd'
            cfg[f'{bus}bus_drive'] = 4 * (1+(conf & max_drive))
            cfg[f'{bus}bus_schmitt'] = bool(conf & self.DRIVE.SCHMITT)
            cfg[f'{bus}bus_slow_slew'] = bool(conf & self.DRIVE.SLOW_SLEW)
            value >>= 4
        for bix in range(4):
            value = self._eeprom[0x1A + bix]
            try:
                cfg[f'cbus_func_{bix}'] = self.CBUSX(value).name
            except ValueError:
                pass

    def _decode_232h(self):
        cfg = self._config
        cfg0, cfg1 = self._eeprom[0x00], self._eeprom[0x01]
        cfg['channel_a_type'] = cfg0 & 0x0F
        cfg['channel_a_driver'] = 'VCP' if (cfg0 & (1 << 4)) else 'D2XX'
        cfg['clock_polarity'] = 'high' if (cfg1 & self.CFG1.CLK_IDLE_STATE) \
                                else 'low'
        cfg['lsb_data'] = bool(cfg1 & self.CFG1.DATA_LSB)
        cfg['flow_control'] = 'on' if (cfg1 & self.CFG1.FLOW_CONTROL) \
                              else 'off'
        cfg['powersave'] = bool(cfg1 & self.DRIVE.PWRSAVE_DIS)
        max_drive = self.DRIVE.LOW.value | self.DRIVE.HIGH.value
        for grp in range(2):
            conf = self._eeprom[0x0c+grp]
            cfg[f'group_{grp}_drive'] = 4 * (1+(conf & max_drive))
            cfg[f'group_{grp}_schmitt'] = \
                bool(conf & self.DRIVE.SCHMITT.value)
            cfg[f'group_{grp}_slow_slew'] = \
                bool(conf & self.DRIVE.SLOW_SLEW.value)
        for bix in range(5):
            value = self._eeprom[0x18 + bix]
            low, high = value & 0x0F, value >> 4
            try:
                cfg[f'cbus_func_{(2*bix)+0}'] = self.CBUSH(low).name
            except ValueError:
                pass
            try:
                cfg[f'cbus_func_{(2*bix)+1}'] = self.CBUSH(high).name
            except ValueError:
                pass

    def _decode_232r(self):
        cfg = self._config
        cfg0 = self._eeprom[0x00]
        cfg['channel_a_driver'] = 'VCP' if (~cfg0 & (1 << 3)) else ''
        cfg['high_current'] = bool(~cfg0 & (1 << 2))
        cfg['external_oscillator'] = cfg0 & 0x02
        for bit in self.UART_BITS:
            value = self._eeprom[0x0B]
            cfg[f'invert_{self.UART_BITS(bit).name}'] = bool(value & bit)
        bix = 0
        while True:
            value = self._eeprom[0x14 + bix]
            low, high = value & 0x0F, value >> 4
            try:
                cfg[f'cbus_func_{(2*bix)+0}'] = self.CBUS(low).name
            except ValueError:
                pass
            if bix == 2:
                break
            try:
                cfg[f'cbus_func_{(2*bix)+1}'] = self.CBUS(high).name
            except ValueError:
                pass
            bix += 1

    def _decode_2232h(self):
        cfg = self._config
        self._decode_x232h(cfg)
        cfg0, cfg1 = self._eeprom[0x00], self._eeprom[0x01]
        cfg['channel_a_type'] = self.CHANNEL(cfg0 & 0x7).name or 'UART'
        cfg['channel_b_type'] = self.CHANNEL(cfg1 & 0x7).name or 'UART'
        cfg['suspend_dbus7'] = bool(cfg1 & self.CFG1.SUSPEND_DBUS7.value)

    def _decode_4232h(self):
        cfg = self._config
        self._decode_x232h(cfg)
        cfg0, cfg1 = self._eeprom[0x00], self._eeprom[0x01]
        cfg['channel_c_driver'] = 'VCP' if ((cfg0 >> 4) & (1 << 3)) else 'D2XX'
        cfg['channel_d_driver'] = 'VCP' if ((cfg1 >> 4) & (1 << 3)) else 'D2XX'
        conf = self._eeprom[0x0B]
        rs485 = self.CHANNEL.RS485
        for chix in range(4):
            cfg[f'channel_{0xa+chix:x}_type'] = (
                'RS485' if conf & (rs485 << chix) else 'UART')

    def _decode_x232h(self, cfg):
        # common code for 2232h, 4232h, 4232ha
        cfg0, cfg1 = self._eeprom[0x00], self._eeprom[0x01]
        cfg['channel_a_driver'] = 'VCP' if (cfg0 & (1 << 3)) else 'D2XX'
        cfg['channel_b_driver'] = 'VCP' if (cfg1 & (1 << 3)) else 'D2XX'
        max_drive = self.DRIVE.LOW.value | self.DRIVE.HIGH.value
        for bix in range(4):
            if not bix & 1:
                val = self._eeprom[0x0c + bix//2]
            else:
                val >>= 4
            cfg[f'group_{bix}_drive'] = 4 * (1+(val & max_drive))
            cfg[f'group_{bix}_schmitt'] = \
                bool(val & self.DRIVE.SCHMITT.value)
            cfg[f'group_{bix}_slow_slew'] = \
                bool(val & self.DRIVE.SLOW_SLEW.value)

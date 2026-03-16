# Copyright (c) 2017-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""MPSSE command debug tracer."""

# pylint: disable=missing-docstring

from binascii import hexlify
from collections import deque
from importlib import import_module
from inspect import currentframe
from logging import getLogger
from string import ascii_uppercase
from struct import unpack as sunpack
from sys import modules
from typing import Union


class FtdiMpsseTracer:
    """FTDI MPSSE protocol decoder."""

    MPSSE_ENGINES = {
        0x0200: 0,
        0x0400: 0,
        0x0500: 0,
        0x0600: 0,
        0x0700: 2,
        0x0800: 2,
        0x0900: 1,
        0x1000: 0,
        0x3600: 2}
    """Count of MPSSE engines."""

    def __init__(self, version):
        count = self.MPSSE_ENGINES[version]
        self._engines = [None] * count

    def send(self, iface: int, buf: Union[bytes, bytearray]) -> None:
        self._get_engine(iface).send(buf)

    def receive(self, iface: int, buf: Union[bytes, bytearray]) -> None:
        self._get_engine(iface).receive(buf)

    def _get_engine(self, iface: int):
        iface -= 1
        try:
            self._engines[iface]
        except IndexError as exc:
            raise ValueError(f'No MPSSE engine available on interface '
                             f'{iface}') from exc
        if not self._engines[iface]:
            self._engines[iface] = FtdiMpsseEngine(iface)
        return self._engines[iface]


class FtdiMpsseEngine:
    """FTDI MPSSE virtual engine

       Far from being complete for now
    """

    COMMAND_PREFIX = \
        'GET SET READ WRITE RW ENABLE DISABLE CLK LOOPBACK SEND DRIVE'

    ST_IDLE = range(1)

    def __init__(self, iface: int):
        self.log = getLogger('pyftdi.mpsse.tracer')
        self._if = iface
        self._trace_tx = bytearray()
        self._trace_rx = bytearray()
        self._state = self.ST_IDLE
        self._clkdiv5 = False
        self._cmd_decoded = True
        self._resp_decoded = True
        self._last_codes = deque()
        self._expect_resp = deque()  # positive: byte, negative: bit count
        self._commands = self._build_commands()

    def send(self, buf: Union[bytes, bytearray]) -> None:
        self._trace_tx.extend(buf)
        while self._trace_tx:
            try:
                code = self._trace_tx[0]
                cmd = self._commands[code]
                if self._cmd_decoded:
                    self.log.debug('[%d]:[Command: %02X: %s]',
                                   self._if, code, cmd)
                cmd_decoder = getattr(self, f'_cmd_{cmd.lower()}')
                rdepth = len(self._expect_resp)
                try:
                    self._cmd_decoded = cmd_decoder()
                except AttributeError as exc:
                    raise ValueError(str(exc)) from exc
                if len(self._expect_resp) > rdepth:
                    self._last_codes.append(code)
                if self._cmd_decoded:
                    continue
                # not enough data in buffer to decode a whole command
                return
            except IndexError:
                self.log.warning('[%d]:Empty buffer on %02X: %s',
                                 self._if, code, cmd)
            except KeyError:
                self.log.warning('[%d]:Unknown command code: %02X',
                                 self._if, code)
            except AttributeError:
                self.log.warning('[%d]:Decoder for command %s [%02X] is not '
                                 'implemented', self._if, cmd, code)
            except ValueError as exc:
                self.log.warning('[%d]:Decoder for command %s [%02X] failed: '
                                 '%s', self._if, cmd, code, exc)
            # on error, flush all buffers
            self.log.warning('Flush TX/RX buffers')
            self._trace_tx = bytearray()
            self._trace_rx = bytearray()
            self._last_codes.clear()

    def receive(self, buf: Union[bytes, bytearray]) -> None:
        self.log.info(' .. %s', hexlify(buf).decode())
        self._trace_rx.extend(buf)
        while self._trace_rx:
            code = None
            try:
                code = self._last_codes.popleft()
                cmd = self._commands[code]
                resp_decoder = getattr(self, f'_resp_{cmd.lower()}')
                self._resp_decoded = resp_decoder()
                if self._resp_decoded:
                    continue
                # not enough data in buffer to decode a whole response
                return
            except IndexError:
                self.log.warning('[%d]:Empty buffer', self._if)
            except KeyError:
                self.log.warning('[%d]:Unknown command code: %02X',
                                 self._if, code)
            except AttributeError:
                self.log.warning('[%d]:Decoder for response %s [%02X] is not '
                                 'implemented', self._if, cmd, code)
            # on error, flush RX buffer
            self.log.warning('[%d]:Flush RX buffer', self._if)
            self._trace_rx = bytearray()
            self._last_codes.clear()

    @classmethod
    def _build_commands(cls):
        # pylint: disable=no-self-argument
        commands = {}
        fdti_mod_name = 'pyftdi.ftdi'
        ftdi_mod = modules.get(fdti_mod_name)
        if not ftdi_mod:
            ftdi_mod = import_module(fdti_mod_name)
        ftdi_type = getattr(ftdi_mod, 'Ftdi')
        for cmd in dir(ftdi_type):
            if cmd[0] not in ascii_uppercase:
                continue
            value = getattr(ftdi_type, cmd)
            if not isinstance(value, int):
                continue
            family = cmd.split('_')[0]
            # pylint: disable=no-member
            if family not in cls.COMMAND_PREFIX.split():
                continue
            commands[value] = cmd
        return commands

    def _cmd_enable_clk_div5(self):
        self.log.info(' [%d]:Enable clock divisor /5', self._if)
        self._clkdiv5 = True
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_disable_clk_div5(self):
        self.log.info(' [%d]:Disable clock divisor /5', self._if)
        self._clkdiv5 = False
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_set_tck_divisor(self):
        if len(self._trace_tx) < 3:
            return False
        value, = sunpack('<H', self._trace_tx[1:3])
        base = 12E6 if self._clkdiv5 else 60E6
        freq = base / ((1 + value) * 2)
        self.log.info(' [%d]:Set frequency %.3fMHZ', self._if, freq/1E6)
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _cmd_loopback_end(self):
        self.log.info(' [%d]:Disable loopback', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_enable_clk_adaptive(self):
        self.log.info(' [%d]:Enable adaptive clock', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_disable_clk_adaptive(self):
        self.log.info(' [%d]:Disable adaptive clock', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_enable_clk_3phase(self):
        self.log.info(' [%d]:Enable 3-phase clock', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_disable_clk_3phase(self):
        self.log.info(' [%d]:Disable 3-phase clock', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_drive_zero(self):
        if len(self._trace_tx) < 3:
            return False
        value, = sunpack('H', self._trace_tx[1:3])
        self.log.info(' [%d]:Open collector [15:0] %04x %s',
                      self._if, value, self.bitfmt(value, 16))
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _cmd_send_immediate(self):
        self.log.debug(' [%d]:Send immediate', self._if)
        self._trace_tx[:] = self._trace_tx[1:]
        return True

    def _cmd_get_bits_low(self):
        self._trace_tx[:] = self._trace_tx[1:]
        self._expect_resp.append(1)
        return True

    def _cmd_get_bits_high(self):
        self._trace_tx[:] = self._trace_tx[1:]
        self._expect_resp.append(1)
        return True

    def _cmd_set_bits_low(self):
        if len(self._trace_tx) < 3:
            return False
        value, direction = sunpack('BB', self._trace_tx[1:3])
        self.log.info(' [%d]:Set gpio[7:0]  %02x %s',
                      self._if, value, self.bm2str(value, direction))
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _cmd_set_bits_high(self):
        if len(self._trace_tx) < 3:
            return False
        value, direction = sunpack('BB', self._trace_tx[1:3])
        self.log.info(' [%d]:Set gpio[15:8] %02x %s',
                      self._if, value, self.bm2str(value, direction))
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _cmd_write_bytes_pve_msb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_write_bytes_nve_msb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_write_bytes_pve_lsb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_write_bytes_nve_lsb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_read_bytes_pve_msb(self):
        return self._decode_input_mpsse_byte_request()

    def _resp_read_bytes_pve_msb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_read_bytes_nve_msb(self):
        return self._decode_input_mpsse_byte_request()

    def _resp_read_bytes_nve_msb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_read_bytes_pve_lsb(self):
        return self._decode_input_mpsse_byte_request()

    def _resp_read_bytes_pve_lsb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_read_bytes_nve_lsb(self):
        return self._decode_input_mpsse_byte_request()

    def _resp_read_bytes_nve_lsb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_rw_bytes_nve_pve_msb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name,
                                               True)

    def _resp_rw_bytes_nve_pve_msb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_rw_bytes_pve_nve_msb(self):
        return self._decode_output_mpsse_bytes(currentframe().f_code.co_name,
                                               True)

    def _resp_rw_bytes_pve_nve_msb(self):
        return self._decode_input_mpsse_bytes(currentframe().f_code.co_name)

    def _cmd_write_bits_pve_msb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_write_bits_nve_msb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_write_bits_pve_lsb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_write_bits_nve_lsb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_read_bits_pve_msb(self):
        return self._decode_input_mpsse_bit_request()

    def _resp_read_bits_pve_msb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_read_bits_nve_msb(self):
        return self._decode_input_mpsse_bit_request()

    def _resp_read_bits_nve_msb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_read_bits_pve_lsb(self):
        return self._decode_input_mpsse_bit_request()

    def _resp_read_bits_pve_lsb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_read_bits_nve_lsb(self):
        return self._decode_input_mpsse_bit_request()

    def _resp_read_bits_nve_lsb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_rw_bits_nve_pve_msb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name,
                                              True)

    def _resp_rw_bits_nve_pve_msb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _cmd_rw_bits_pve_nve_msb(self):
        return self._decode_output_mpsse_bits(currentframe().f_code.co_name,
                                              True)

    def _resp_rw_bits_pve_nve_msb(self):
        return self._decode_input_mpsse_bits(currentframe().f_code.co_name)

    def _resp_get_bits_low(self):
        if self._trace_rx:
            return False
        value = self._trace_rx[0]
        self.log.info(' [%d]:Get gpio[7:0]  %02x %s',
                      self._if, value, self.bm2str(value, 0xFF))
        self._trace_rx[:] = self._trace_rx[1:]
        return True

    def _resp_get_bits_high(self):
        if self._trace_rx:
            return False
        value = self._trace_rx[0]
        self.log.info(' [%d]:Get gpio[15:8] %02x %s',
                      self._if, value, self.bm2str(value, 0xFF))
        self._trace_rx[:] = self._trace_rx[1:]
        return True

    def _decode_output_mpsse_bytes(self, caller, expect_rx=False):
        if len(self._trace_tx) < 4:
            return False
        length = sunpack('<H', self._trace_tx[1:3])[0] + 1
        if len(self._trace_tx) < 4 + length:
            return False
        if expect_rx:
            self._expect_resp.append(length)
        payload = self._trace_tx[3:3+length]
        funcname = caller[5:].title().replace('_', '')
        self.log.info(' [%d]:%s> (%d) %s',
                      self._if, funcname, length,
                      hexlify(payload).decode('utf8'))
        self._trace_tx[:] = self._trace_tx[3+length:]
        return True

    def _decode_output_mpsse_bits(self, caller, expect_rx=False):
        if len(self._trace_tx) < 3:
            return False
        bitlen = self._trace_tx[1] + 1
        if expect_rx:
            self._expect_resp.append(-bitlen)
        payload = self._trace_tx[2]
        funcname = caller[5:].title().replace('_', '')
        msb = caller[5:][-3].lower() == 'm'
        self.log.info(' %s> (%d) %s',
                      funcname, bitlen, self.bit2str(payload, bitlen, msb))
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _decode_input_mpsse_byte_request(self):
        if len(self._trace_tx) < 3:
            return False
        length = sunpack('<H', self._trace_tx[1:3])[0] + 1
        self._expect_resp.append(length)
        self._trace_tx[:] = self._trace_tx[3:]
        return True

    def _decode_input_mpsse_bit_request(self):
        if len(self._trace_tx) < 2:
            return False
        bitlen = self._trace_tx[1] + 1
        self._expect_resp.append(-bitlen)
        self._trace_tx[:] = self._trace_tx[2:]
        return True

    def _decode_input_mpsse_bytes(self, caller):
        if not self._expect_resp:
            self.log.warning('[%d]:Response w/o request?', self._if)
            return False
        if self._expect_resp[0] < 0:
            self.log.warning('[%d]:Handling byte request w/ bit length',
                             self._if)
            return False
        if len(self._trace_rx) < self._expect_resp[0]:  # peek
            return False
        length = self._expect_resp.popleft()
        payload = self._trace_rx[:length]
        self._trace_rx[:] = self._trace_rx[length:]
        funcname = caller[5:].title().replace('_', '')
        self.log.info(' %s< (%d) %s',
                      funcname, length, hexlify(payload).decode('utf8'))
        return True

    def _decode_input_mpsse_bits(self, caller):
        if not self._expect_resp:
            self.log.warning('[%d]:Response w/o request?', self._if)
            return False
        if not self._trace_rx:  # peek
            return False
        if self._expect_resp[0] > 0:
            self.log.warning('[%d]:Handling bit request w/ byte length',
                             self._if)
        bitlen = -self._expect_resp.popleft()
        payload = self._trace_rx[0]
        self._trace_rx[:] = self._trace_rx[1:]
        funcname = caller[5:].title().replace('_', '')
        msb = caller[5:][-3].lower() == 'm'
        self.log.info(' %s< (%d) %s',
                      funcname, bitlen, self.bit2str(payload, bitlen, msb))
        return True

    @classmethod
    def bit2str(cls, value: int, count: int, msb: bool, hiz: str = '_') -> str:
        mask = (1 << count) - 1
        if msb:
            mask <<= 8 - count
        return cls.bm2str(value, mask, hiz)

    @classmethod
    def bm2str(cls, value: int, mask: int, hiz: str = '_') -> str:
        vstr = cls.bitfmt(value, 8)
        mstr = cls.bitfmt(mask, 8)
        return ''.join([m == '1' and v or hiz for v, m in zip(vstr, mstr)])

    @classmethod
    def bitfmt(cls, value, width):
        return format(value, f'0{width}b')

    # rw_bytes_pve_pve_lsb
    # rw_bytes_pve_nve_lsb
    # rw_bytes_nve_pve_lsb
    # rw_bytes_nve_nve_lsb
    # rw_bits_pve_pve_lsb
    # rw_bits_pve_nve_lsb
    # rw_bits_nve_pve_lsb
    # rw_bits_nve_nve_lsb
    # write_bits_tms_pve
    # write_bits_tms_nve
    # rw_bits_tms_pve_pve
    # rw_bits_tms_nve_pve
    # rw_bits_tms_pve_nve
    # rw_bits_tms_nve_nve

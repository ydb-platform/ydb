# Copyright (c) 2010-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2016, Emmanuel Bouaziz <ebouaziz@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""JTAG support for PyFdti"""

# pylint: disable=invalid-name
# pylint: disable=missing-function-docstring

from time import sleep
from typing import List, Tuple, Union
from .ftdi import Ftdi
from .bits import BitSequence


class JtagError(Exception):
    """Generic JTAG error."""


class JtagState:
    """Test Access Port controller state"""

    def __init__(self, name: str, modes: Tuple[str, str]):
        self.name = name
        self.modes = modes
        self.exits = [self, self]  # dummy value before initial configuration

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def setx(self, fstate: 'JtagState', tstate: 'JtagState'):
        self.exits = [fstate, tstate]

    def getx(self, event):
        x = int(bool(event))
        return self.exits[x]

    def is_of(self, mode: str) -> bool:
        return mode in self.modes


class JtagStateMachine:
    """Test Access Port controller state machine."""

    def __init__(self):
        self.states = {}
        for s, modes in [('test_logic_reset', ('reset', ' idle')),
                         ('run_test_idle', ('idle',)),
                         ('select_dr_scan', ('dr',)),
                         ('capture_dr', ('dr', 'shift', 'capture')),
                         ('shift_dr', ('dr', 'shift')),
                         ('exit_1_dr', ('dr', 'update', 'pause')),
                         ('pause_dr', ('dr', 'pause')),
                         ('exit_2_dr', ('dr', 'shift', 'udpate')),
                         ('update_dr', ('dr', 'idle')),
                         ('select_ir_scan', ('ir',)),
                         ('capture_ir', ('ir', 'shift', 'capture')),
                         ('shift_ir', ('ir', 'shift')),
                         ('exit_1_ir', ('ir', 'udpate', 'pause')),
                         ('pause_ir', ('ir', 'pause')),
                         ('exit_2_ir', ('ir', 'shift', 'update')),
                         ('update_ir', ('ir', 'idle'))]:
            self.states[s] = JtagState(s, modes)
        self['test_logic_reset'].setx(self['run_test_idle'],
                                      self['test_logic_reset'])
        self['run_test_idle'].setx(self['run_test_idle'],
                                   self['select_dr_scan'])
        self['select_dr_scan'].setx(self['capture_dr'],
                                    self['select_ir_scan'])
        self['capture_dr'].setx(self['shift_dr'], self['exit_1_dr'])
        self['shift_dr'].setx(self['shift_dr'], self['exit_1_dr'])
        self['exit_1_dr'].setx(self['pause_dr'], self['update_dr'])
        self['pause_dr'].setx(self['pause_dr'], self['exit_2_dr'])
        self['exit_2_dr'].setx(self['shift_dr'], self['update_dr'])
        self['update_dr'].setx(self['run_test_idle'],
                               self['select_dr_scan'])
        self['select_ir_scan'].setx(self['capture_ir'],
                                    self['test_logic_reset'])
        self['capture_ir'].setx(self['shift_ir'], self['exit_1_ir'])
        self['shift_ir'].setx(self['shift_ir'], self['exit_1_ir'])
        self['exit_1_ir'].setx(self['pause_ir'], self['update_ir'])
        self['pause_ir'].setx(self['pause_ir'], self['exit_2_ir'])
        self['exit_2_ir'].setx(self['shift_ir'], self['update_ir'])
        self['update_ir'].setx(self['run_test_idle'], self['select_dr_scan'])
        self._current = self['test_logic_reset']

    def __getitem__(self, name: str) -> JtagState:
        return self.states[name]

    def state(self) -> JtagState:
        return self._current

    def state_of(self, mode: str) -> bool:
        return self._current.is_of(mode)

    def reset(self):
        self._current = self['test_logic_reset']

    def find_path(self, target: Union[JtagState, str],
                  source: Union[JtagState, str, None] = None) \
            -> List[JtagState]:
        """Find the shortest event sequence to move from source state to
           target state. If source state is not specified, used the current
           state.

           :return: the list of states, including source and target states.
        """
        if source is None:
            source = self.state()
        if isinstance(source, str):
            source = self[source]
        if isinstance(target, str):
            target = self[target]

        def next_path(state, target, path):
            # this test match the target, path is valid
            if state == target:
                return path+[state]
            # candidate paths
            paths = []
            for x in state.exits:
                # next state is self (loop around), kill the path
                if x == state:
                    continue
                # next state already in upstream (loop back), kill the path
                if x in path:
                    continue
                # try the current path
                npath = next_path(x, target, path + [state])
                # downstream is a valid path, store it
                if npath:
                    paths.append(npath)
            # keep the shortest path
            return min(((len(p), p) for p in paths), key=lambda x: x[0])[1] \
                if paths else []
        return next_path(source, target, [])

    @classmethod
    def get_events(cls, path):
        """Build up an event sequence from a state sequence, so that the
           resulting event sequence allows the JTAG state machine to advance
           from the first state to the last one of the input sequence"""
        events = []
        for s, d in zip(path[:-1], path[1:]):
            for e, x in enumerate(s.exits):
                if x == d:
                    events.append(e)
        if len(events) != len(path) - 1:
            raise JtagError("Invalid path")
        return BitSequence(events)

    def handle_events(self, events):
        for event in events:
            self._current = self._current.getx(event)


class JtagController:
    """JTAG master of an FTDI device"""

    TCK_BIT = 0x01   # FTDI output
    TDI_BIT = 0x02   # FTDI output
    TDO_BIT = 0x04   # FTDI input
    TMS_BIT = 0x08   # FTDI output
    TRST_BIT = 0x10  # FTDI output, not available on 2232 JTAG debugger
    JTAG_MASK = 0x1f
    FTDI_PIPE_LEN = 512

    # Private API
    def __init__(self, trst: bool = False, frequency: float = 3.0E6):
        """
        trst uses the nTRST optional JTAG line to hard-reset the TAP
          controller
        """
        self._ftdi = Ftdi()
        self._trst = trst
        self._frequency = frequency
        self.direction = (JtagController.TCK_BIT |
                          JtagController.TDI_BIT |
                          JtagController.TMS_BIT |
                          (self._trst and JtagController.TRST_BIT or 0))
        self._last = None  # Last deferred TDO bit
        self._write_buff = bytearray()

    # Public API
    def configure(self, url: str) -> None:
        """Configure the FTDI interface as a JTAG controller"""
        self._ftdi.open_mpsse_from_url(
            url, direction=self.direction, frequency=self._frequency)
        # FTDI requires to initialize all GPIOs before MPSSE kicks in
        cmd = bytearray((Ftdi.SET_BITS_LOW, 0x0, self.direction))
        self._ftdi.write_data(cmd)

    def close(self, freeze: bool = False) -> None:
        """Close the JTAG interface/port.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close. This means the port is left with
                          its current configuration and output signals.
                          This feature should not be used except for very
                          specific needs.
        """
        if self._ftdi.is_connected:
            self._ftdi.close(freeze)

    def purge(self) -> None:
        self._ftdi.purge_buffers()

    def reset(self, sync: bool = False) -> None:
        """Reset the attached TAP controller.
           sync sends the command immediately (no caching)
        """
        # we can either send a TRST HW signal or perform 5 cycles with TMS=1
        # to move the remote TAP controller back to 'test_logic_reset' state
        # do both for now
        if not self._ftdi.is_connected:
            raise JtagError("FTDI controller terminated")
        if self._trst:
            # nTRST
            value = 0
            cmd = bytearray((Ftdi.SET_BITS_LOW, value, self.direction))
            self._ftdi.write_data(cmd)
            sleep(0.1)
            # nTRST should be left to the high state
            value = JtagController.TRST_BIT
            cmd = bytearray((Ftdi.SET_BITS_LOW, value, self.direction))
            self._ftdi.write_data(cmd)
            sleep(0.1)
        # TAP reset (even with HW reset, could be removed though)
        self.write_tms(BitSequence('11111'))
        if sync:
            self.sync()

    def sync(self) -> None:
        if not self._ftdi.is_connected:
            raise JtagError("FTDI controller terminated")
        if self._write_buff:
            self._ftdi.write_data(self._write_buff)
            self._write_buff = bytearray()

    def write_tms(self, tms: BitSequence,
                  should_read: bool = False) -> None:
        """Change the TAP controller state"""
        if not isinstance(tms, BitSequence):
            raise JtagError('Expect a BitSequence')
        length = len(tms)
        if not 0 < length < 8:
            raise JtagError('Invalid TMS length')
        out = BitSequence(tms, length=8)
        # apply the last TDO bit
        if self._last is not None:
            out[7] = self._last
        # print("TMS", tms, (self._last is not None) and 'w/ Last' or '')
        # reset last bit
        self._last = None
        if should_read:
            cmd = bytearray((Ftdi.RW_BITS_TMS_PVE_NVE, length-1, out.tobyte()))
        else:
            cmd = bytearray((Ftdi.WRITE_BITS_TMS_NVE, length-1, out.tobyte()))
        self._stack_cmd(cmd)
        if should_read:
            self.sync()

    def read(self, length: int) -> BitSequence:
        """Read out a sequence of bits from TDO."""
        byte_count = length//8
        bit_count = length-8*byte_count
        bs = BitSequence()
        if byte_count:
            bytes_ = self._read_bytes(byte_count)
            bs.append(bytes_)
        if bit_count:
            bits = self._read_bits(bit_count)
            bs.append(bits)
        return bs

    def write(self, out: Union[BitSequence, str], use_last: bool = True):
        """Write a sequence of bits to TDI"""
        if isinstance(out, str):
            if len(out) > 1:
                self._write_bytes_raw(out[:-1])
                out = out[-1]
            out = BitSequence(bytes_=out)
        elif not isinstance(out, BitSequence):
            out = BitSequence(out)
        if use_last:
            (out, self._last) = (out[:-1], bool(out[-1]))
        byte_count = len(out)//8
        pos = 8*byte_count
        bit_count = len(out)-pos
        if byte_count:
            self._write_bytes(out[:pos])
        if bit_count:
            self._write_bits(out[pos:])

    def write_with_read(self, out: BitSequence,
                        use_last: bool = False) -> int:
        """Write the given BitSequence while reading the same
           number of bits into the FTDI read buffer.
           Returns the number of bits written."""
        if not isinstance(out, BitSequence):
            return JtagError('Expect a BitSequence')
        if use_last:
            (out, self._last) = (out[:-1], int(out[-1]))
        byte_count = len(out)//8
        pos = 8*byte_count
        bit_count = len(out)-pos
        if not byte_count and not bit_count:
            raise JtagError("Nothing to shift")
        if byte_count:
            blen = byte_count-1
            # print("RW OUT %s" % out[:pos])
            cmd = bytearray((Ftdi.RW_BYTES_PVE_NVE_LSB,
                             blen, (blen >> 8) & 0xff))
            cmd.extend(out[:pos].tobytes(msby=True))
            self._stack_cmd(cmd)
            # print("push %d bytes" % byte_count)
        if bit_count:
            # print("RW OUT %s" % out[pos:])
            cmd = bytearray((Ftdi.RW_BITS_PVE_NVE_LSB, bit_count-1))
            cmd.append(out[pos:].tobyte())
            self._stack_cmd(cmd)
            # print("push %d bits" % bit_count)
        return len(out)

    def read_from_buffer(self, length) -> BitSequence:
        """Read the specified number of bits from the FTDI read buffer."""
        self.sync()
        bs = BitSequence()
        byte_count = length//8
        pos = 8*byte_count
        bit_count = length-pos
        if byte_count:
            data = self._ftdi.read_data_bytes(byte_count, 4)
            if not data:
                raise JtagError('Unable to read data from FTDI')
            byteseq = BitSequence(bytes_=data, length=8*byte_count)
            # print("RW IN %s" % byteseq)
            bs.append(byteseq)
            # print("pop %d bytes" % byte_count)
        if bit_count:
            data = self._ftdi.read_data_bytes(1, 4)
            if not data:
                raise JtagError('Unable to read data from FTDI')
            byte = data[0]
            # need to shift bits as they are shifted in from the MSB in FTDI
            byte >>= 8-bit_count
            bitseq = BitSequence(byte, length=bit_count)
            bs.append(bitseq)
            # print("pop %d bits" % bit_count)
        if len(bs) != length:
            raise ValueError("Internal error")
        return bs

    @property
    def ftdi(self) -> Ftdi:
        """Return the Ftdi instance.

           :return: the Ftdi instance
        """
        return self._ftdi

    def _stack_cmd(self, cmd: Union[bytes, bytearray]):
        if not isinstance(cmd, (bytes, bytearray)):
            raise TypeError('Expect bytes or bytearray')
        if not self._ftdi:
            raise JtagError("FTDI controller terminated")
        # Currrent buffer + new command + send_immediate
        if (len(self._write_buff)+len(cmd)+1) >= JtagController.FTDI_PIPE_LEN:
            self.sync()
        self._write_buff.extend(cmd)

    def _read_bits(self, length: int):
        """Read out bits from TDO"""
        if length > 8:
            raise JtagError("Cannot fit into FTDI fifo")
        cmd = bytearray((Ftdi.READ_BITS_NVE_LSB, length-1))
        self._stack_cmd(cmd)
        self.sync()
        data = self._ftdi.read_data_bytes(1, 4)
        # need to shift bits as they are shifted in from the MSB in FTDI
        byte = data[0] >> 8-length
        bs = BitSequence(byte, length=length)
        # print("READ BITS %s" % bs)
        return bs

    def _write_bits(self, out: BitSequence) -> None:
        """Output bits on TDI"""
        length = len(out)
        byte = out.tobyte()
        # print("WRITE BITS %s" % out)
        cmd = bytearray((Ftdi.WRITE_BITS_NVE_LSB, length-1, byte))
        self._stack_cmd(cmd)

    def _read_bytes(self, length: int) -> BitSequence:
        """Read out bytes from TDO"""
        if length > JtagController.FTDI_PIPE_LEN:
            raise JtagError("Cannot fit into FTDI fifo")
        alen = length-1
        cmd = bytearray((Ftdi.READ_BYTES_NVE_LSB, alen & 0xff,
                         (alen >> 8) & 0xff))
        self._stack_cmd(cmd)
        self.sync()
        data = self._ftdi.read_data_bytes(length, 4)
        bs = BitSequence(bytes_=data, length=8*length)
        # print("READ BYTES %s" % bs)
        return bs

    def _write_bytes(self, out: BitSequence):
        """Output bytes on TDI"""
        bytes_ = out.tobytes(msby=True)  # don't ask...
        olen = len(bytes_)-1
        # print("WRITE BYTES %s" % out)
        cmd = bytearray((Ftdi.WRITE_BYTES_NVE_LSB, olen & 0xff,
                         (olen >> 8) & 0xff))
        cmd.extend(bytes_)
        self._stack_cmd(cmd)

    def _write_bytes_raw(self, out: BitSequence):
        """Output bytes on TDI"""
        olen = len(out)-1
        cmd = bytearray((Ftdi.WRITE_BYTES_NVE_LSB, olen & 0xff,
                         (olen >> 8) & 0xff))
        cmd.extend(out)
        self._stack_cmd(cmd)


class JtagEngine:
    """High-level JTAG engine controller"""

    def __init__(self, trst: bool = False, frequency: float = 3E06):
        self._ctrl = JtagController(trst, frequency)
        self._sm = JtagStateMachine()
        self._seq = bytearray()

    @property
    def state_machine(self):
        return self._sm

    @property
    def controller(self):
        return self._ctrl

    def configure(self, url: str) -> None:
        """Configure the FTDI interface as a JTAG controller"""
        self._ctrl.configure(url)

    def close(self, freeze: bool = False) -> None:
        """Terminate a JTAG session/connection.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close.
        """
        self._ctrl.close(freeze)

    def purge(self) -> None:
        """Purge low level HW buffers"""
        self._ctrl.purge()

    def reset(self) -> None:
        """Reset the attached TAP controller"""
        self._ctrl.reset()
        self._sm.reset()

    def write_tms(self, out, should_read=False) -> None:
        """Change the TAP controller state"""
        self._ctrl.write_tms(out, should_read=should_read)

    def read(self, length):
        """Read out a sequence of bits from TDO"""
        return self._ctrl.read(length)

    def write(self, out, use_last=False) -> None:
        """Write a sequence of bits to TDI"""
        self._ctrl.write(out, use_last)

    def get_available_statenames(self):
        """Return a list of supported state name"""
        return [str(s) for s in self._sm.states]

    def change_state(self, statename) -> None:
        """Advance the TAP controller to the defined state"""
        # find the state machine path to move to the new instruction
        path = self._sm.find_path(statename)
        # convert the path into an event sequence
        events = self._sm.get_events(path)
        # update the remote device tap controller
        self._ctrl.write_tms(events)
        # update the current state machine's state
        self._sm.handle_events(events)

    def go_idle(self) -> None:
        """Change the current TAP controller to the IDLE state"""
        self.change_state('run_test_idle')

    def write_ir(self, instruction) -> None:
        """Change the current instruction of the TAP controller"""
        self.change_state('shift_ir')
        self._ctrl.write(instruction)
        self.change_state('update_ir')

    def capture_ir(self) -> None:
        """Capture the current instruction from the TAP controller"""
        self.change_state('capture_ir')

    def write_dr(self, data) -> None:
        """Change the data register of the TAP controller"""
        self.change_state('shift_dr')
        self._ctrl.write(data)
        self.change_state('update_dr')

    def read_dr(self, length: int) -> BitSequence:
        """Read the data register from the TAP controller"""
        self.change_state('shift_dr')
        data = self._ctrl.read(length)
        self.change_state('update_dr')
        return data

    def capture_dr(self) -> None:
        """Capture the current data register from the TAP controller"""
        self.change_state('capture_dr')

    def sync(self) -> None:
        self._ctrl.sync()

    def shift_register(self, out: BitSequence) -> BitSequence:
        if not self._sm.state_of('shift'):
            raise JtagError(f'Invalid state: {self._sm.state()}')
        if self._sm.state_of('capture'):
            bs = BitSequence(False)
            self._ctrl.write_tms(bs)
            self._sm.handle_events(bs)

        bits_out = self._ctrl.write_with_read(out)
        bs = self._ctrl.read_from_buffer(bits_out)
        if len(bs) != len(out):
            raise ValueError("Internal error")

        return bs

    def shift_and_update_register(self, out: BitSequence) -> BitSequence:
        """Shift a BitSequence into the current register and retrieve the
           register output, advancing the state to update_*r"""
        if not self._sm.state_of('shift'):
            raise JtagError(f'Invalid state: {self._sm.state()}')
        if self._sm.state_of('capture'):
            bs = BitSequence(False)
            self._ctrl.write_tms(bs)
            self._sm.handle_events(bs)

        # Write with read using the last bit for next TMS transition
        bits_out = self._ctrl.write_with_read(out, use_last=True)

        # Advance the state from shift to update
        events = BitSequence('11')
        self.write_tms(events, should_read=True)
        # (write_tms calls sync())
        # update the current state machine's state
        self._sm.handle_events(events)

        # Read the bits clocked out as part of the initial write
        bs = self._ctrl.read_from_buffer(bits_out)

        # Read the last two bits clocked out with TMS above
        # (but only use the lowest bit in the return data)
        last_bits = self._ctrl.read_from_buffer(2)
        bs.append(BitSequence((last_bits.tobyte() & 0x1), length=1))
        if len(bs) != len(out):
            raise ValueError("Internal error")

        return bs


class JtagTool:
    """A helper class with facility functions"""

    def __init__(self, engine):
        self._engine = engine

    def idcode(self) -> None:
        idcode = self._engine.read_dr(32)
        self._engine.go_idle()
        return int(idcode)

    def preload(self, bsdl, data) -> None:
        instruction = bsdl.get_jtag_ir('preload')
        self._engine.write_ir(instruction)
        self._engine.write_dr(data)
        self._engine.go_idle()

    def sample(self, bsdl):
        instruction = bsdl.get_jtag_ir('sample')
        self._engine.write_ir(instruction)
        data = self._engine.read_dr(bsdl.get_boundary_length())
        self._engine.go_idle()
        return data

    def extest(self, bsdl) -> None:
        instruction = bsdl.get_jtag_ir('extest')
        self._engine.write_ir(instruction)

    def readback(self, bsdl):
        data = self._engine.read_dr(bsdl.get_boundary_length())
        self._engine.go_idle()
        return data

    def detect_register_size(self) -> int:
        # Freely inpired from UrJTAG
        stm = self._engine.state_machine
        if not stm.state_of('shift'):
            raise JtagError(f'Invalid state: {stm.state()}')
        if stm.state_of('capture'):
            bs = BitSequence(False)
            self._engine.controller.write_tms(bs)
            stm.handle_events(bs)
        MAX_REG_LEN = 1024
        PATTERN_LEN = 8
        stuck = None
        for length in range(1, MAX_REG_LEN):
            print(f'Testing for length {length}')
            if length > 5:
                raise ValueError(f'Abort detection over reg length {length}')
            zero = BitSequence(length=length)
            inj = BitSequence(length=length+PATTERN_LEN)
            inj.inc()
            ok = False
            for _ in range(1, 1 << PATTERN_LEN):
                ok = False
                self._engine.write(zero, False)
                rcv = self._engine.shift_register(inj)
                try:
                    tdo = rcv.invariant()
                except ValueError:
                    tdo = None
                if stuck is None:
                    stuck = tdo
                if stuck != tdo:
                    stuck = None
                rcv >>= length
                if rcv == inj:
                    ok = True
                else:
                    break
                inj.inc()
            if ok:
                print(f'Register detected length: {length}')
                return length
        if stuck is not None:
            raise JtagError('TDO seems to be stuck')
        raise JtagError('Unable to detect register length')

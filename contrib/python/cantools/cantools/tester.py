# The tester module.

import queue
import time
from collections import UserDict
from collections.abc import Mapping

import can

from cantools.database.can.message import Message as MessageCls

from .errors import Error


class DecodedMessage:
    """A decoded message.

    """

    def __init__(self, name, signals):
        self.name = name
        self.signals = signals


class Messages(UserDict):
    def __setitem__(self, message_name, value):
        if getattr(self, '_frozen', False):
            if message_name not in self.data:
                raise KeyError(message_name)
        self.data[message_name] = value

    def __missing__(self, key):
        raise Error(f"invalid message name '{key}'")


def _invert_signal_tree(
        tree: list,
        cur_mpx: dict | None = None,
        ret: dict | None = None
) -> dict:

    """The tree is laid out with two kinds of dicts.  Single-element dict
    keyed by string -> multiplexer, which is own dict keyed by
    integers.

    """

    if ret is None:
        ret = {}

    if cur_mpx is None:
        cur_mpx = {}

    for sigs in tree:
        if isinstance(sigs, Mapping): # outer signal keyed by muliplexer name
            (mpx_name, mpx_vals), = sigs.items()
            for mpx_val, sig_tree in mpx_vals.items(): # inner signal
                #                          keyed by multiplexer values
                next_mpx = cur_mpx.copy()
                next_mpx[mpx_name] = mpx_val
                _invert_signal_tree(sig_tree, next_mpx, ret)

        elif isinstance(sigs, str):
            ret.setdefault(sigs,[]).append(set(cur_mpx.items()))
        else:
            raise TypeError(repr(sigs))

    return ret

def invert_signal_tree(tree: list) -> dict:
    """Return a mapping of signals to the multiplex settings that will
    yield the signal.

    {signal: [{mplexers}, {mplexers...}]}

    """
    return _invert_signal_tree(tree)

class Listener(can.Listener):

    def __init__(self, database, messages, input_queue, on_message):
        self._database = database
        self._messages = messages
        self._input_queue = input_queue
        self._on_message = on_message

    def on_message_received(self, msg):
        if msg.is_error_frame or msg.is_remote_frame:
            return

        try:
            database_message = self._database.get_message_by_frame_id(msg.arbitration_id, msg.is_extended_id)
        except KeyError:
            return

        if database_message.name not in self._messages:
            return

        message = self._messages[database_message.name]

        if not message.enabled:
            return

        decoded = DecodedMessage(database_message.name,
                                 database_message.decode(msg.data,
                                                         message.decode_choices,
                                                         message.scaling))

        if self._on_message:
            self._on_message(decoded)

        self._input_queue.put(decoded)


class Message(UserDict):

    def __init__(self,
                 database: MessageCls,
                 can_bus: can.BusABC,
                 input_list: list[MessageCls],
                 input_queue: queue.Queue[DecodedMessage],
                 decode_choices: bool,
                 scaling: bool,
                 padding: bool,
                 strict: bool = True) -> None:
        super().__init__()
        self.database = database
        self._mplex_map = invert_signal_tree(database.signal_tree)
        self._can_bus = can_bus
        self._input_queue = input_queue
        self.decode_choices = decode_choices
        self.scaling = scaling
        self.padding = padding
        self.strict = strict
        self._input_list = input_list
        self.enabled = True
        self._can_message = None
        self._periodic_task = None
        self._signal_names = {s.name for s in self.database.signals}
        self.update(self._prepare_initial_signal_values())

    @property
    def periodic(self):
        return self.database.cycle_time is not None

    def __getitem__(self, signal_name):
        return self.data[signal_name]

    def __setitem__(self, signal_name, value):
        if signal_name not in self._signal_names:
            raise KeyError(signal_name)
        self.data[signal_name] = value
        self._update_can_message()

    def update(self, signals):
        s = dict(signals)
        new_signal_names = set(s) - self._signal_names
        if new_signal_names:
            raise KeyError(repr(new_signal_names))

        self.data.update(s)
        self._update_can_message()

    def send(self, signals=None):
        if signals is not None:
            self.update(signals)

        self._can_bus.send(self._can_message)

    def expect(self, signals=None, timeout=None, discard_other_messages=True):
        if signals is None:
            signals = {}

        decoded = self._expect_input_list(signals, discard_other_messages)

        if decoded is None:
            decoded = self._expect_input_queue(signals,
                                               timeout,
                                               discard_other_messages)

        return decoded

    def _expect_input_list(self, signals, discard_other_messages):
        other_messages = []

        while len(self._input_list) > 0:
            message = self._input_list.pop(0)
            decoded = self._filter_expected_message(message, signals)

            if decoded is not None:
                break

            other_messages.append(message)
        else:
            decoded = None

        if not discard_other_messages:
            other_messages += self._input_list
            del self._input_list[:]
            self._input_list.extend(other_messages)

        return decoded

    def _expect_input_queue(self, signals, timeout, discard_other_messages):
        if timeout is not None:
            end_time = time.time() + timeout
            remaining_time = timeout
        else:
            remaining_time = None

        while True:
            try:
                message = self._input_queue.get(timeout=remaining_time)
            except queue.Empty:
                return

            decoded = self._filter_expected_message(message, signals)

            if decoded is not None:
                return decoded

            if not discard_other_messages:
                self._input_list.append(message)

            if timeout is not None:
                remaining_time = end_time - time.time()

                if remaining_time <= 0:
                    return

    def _filter_expected_message(self, message, signals):
        if message.name == self.database.name:
            if all(message.signals[name] == signals[name] for name in signals):
                return message.signals

    def send_periodic_start(self):
        if not self.enabled:
            return

        self._periodic_task = self._can_bus.send_periodic(
            self._can_message,
            self.database.cycle_time / 1000.0)

    def send_periodic_stop(self):
        if self._periodic_task is not None:
            self._periodic_task.stop()
            self._periodic_task = None

    def _update_can_message(self):
        arbitration_id = self.database.frame_id
        extended_id = self.database.is_extended_frame
        pruned_data = self.database.gather_signals(self.data)
        data = self.database.encode(pruned_data,
                                    self.scaling,
                                    self.padding,
                                    strict=self.strict)
        self._can_message = can.Message(arbitration_id=arbitration_id,
                                        is_extended_id=extended_id,
                                        data=data,
                                        is_fd=self.database.is_fd,
                                        dlc=self.database.length,
                                        check=True)

        if self._periodic_task is not None:
            self._periodic_task.modify_data(self._can_message)

    def _prepare_initial_signal_values(self):
        initial_sig_values = {}

        # Choose a valid set of mux settings
        mplex_settings = {}
        for m0 in reversed(self._mplex_map.values()):
            for m1 in m0:
                mplex_settings.update(m1)

        for signal in self.database.signals:
            minimum = 0 if not signal.minimum else signal.minimum
            maximum = 0 if not signal.maximum else signal.maximum
            if signal.initial:
                # use initial signal value (if set)
                initial_sig_values[signal.name] = signal.initial
            elif signal.is_multiplexer:
                initial_sig_values[signal.name] = mplex_settings.get(signal.name, 0)
            elif minimum <= 0 <= maximum:
                # use 0 if in allowed range
                initial_sig_values[signal.name] = 0
            else:
                # set at least some default value
                initial_sig_values[signal.name] = minimum
        return initial_sig_values


class Tester:
    """Test given node `dut_name` on given CAN bus `bus_name`.

    `database` is a :class:`~cantools.database.can.Database` instance.

    `can_bus` a CAN bus object, normally created using the python-can
    package.

    The `on_message` callback is called for every successfully decoded
    received message. It is called with one argument, an
    :class:`~cantools.tester.DecodedMessage` instance.

    Here is an example of how to create a tester:

    >>> import can
    >>> import cantools
    >>> can.rc['interface'] = 'socketcan'
    >>> can.rc['channel'] = 'vcan0'
    >>> can_bus = can.interface.Bus()
    >>> database = cantools.database.load_file('tests/files/tester.kcd')
    >>> tester = cantools.tester.Tester('PeriodicConsumer', database, can_bus, 'PeriodicBus')

    """

    def __init__(self,
                 dut_name,
                 database,
                 can_bus,
                 bus_name=None,
                 on_message=None,
                 decode_choices=True,
                 scaling=True,
                 padding=False,
                 strict=True):
        self._dut_name = dut_name
        self._bus_name = bus_name
        self._database = database
        self._can_bus = can_bus
        self._input_list = []
        self._input_queue = queue.Queue()
        self._messages = Messages()
        self._is_running = False

        # DUT name validation.
        node_names = [node.name for node in database.nodes]

        if dut_name and not any(name == dut_name for name in node_names):
            raise Error(f"expected DUT name in {node_names}, but got '{dut_name}'")

        # BUS name validation.
        bus_names = [bus.name for bus in database.buses]

        if len(bus_names) == 0:
            if bus_name is not None:
                raise Error(
                    f"expected bus name None as there are no buses defined in "
                    f"the database, but got '{bus_name}'")
        elif not any(name == bus_name for name in bus_names):
            raise Error(f"expected bus name in {bus_names}, but got '{bus_name}'")

        for message in database.messages:
            if message.bus_name == bus_name:
                self._messages[message.name] = Message(message,
                                                       can_bus,
                                                       self._input_list,
                                                       self._input_queue,
                                                       decode_choices,
                                                       scaling,
                                                       padding,
                                                       strict=strict)

        listener = Listener(self._database,
                            self._messages,
                            self._input_queue,
                            on_message)
        self._notifier = can.Notifier(can_bus, [listener])
        self._messages._frozen = True

    def start(self):
        """Start the tester. Starts sending enabled periodic messages.

        >>> tester.start()

        """

        for message in self._messages.values():
            if self._dut_name and self._dut_name in message.database.senders:
                continue

            if not message.periodic:
                continue

            message.send_periodic_start()

        self._is_running = True

    def stop(self):
        """Stop the tester. Periodic messages will not be sent after this
        call. Call :meth:`~cantools.tester.Tester.start()` to resume a
        stopped tester.

        >>> tester.stop()

        """

        for message in self._messages.values():
            message.send_periodic_stop()

        self._is_running = False

    @property
    def messages(self):
        """Set and get signals in messages. Set signals takes effect
        immediately for started enabled periodic messages. Call
        :meth:`~cantools.tester.Tester.send()` for other messages.

        >>> periodic_message = tester.messages['PeriodicMessage1']
        >>> periodic_message
        {'Signal1': 0, 'Signal2': 0}
        >>> periodic_message['Signal1'] = 1
        >>> periodic_message.update({'Signal1': 2, 'Signal2': 5})
        >>> periodic_message
        {'Signal1': 2, 'Signal2': 5}

        """

        return self._messages

    def enable(self, message_name):
        """Enable given message `message_name` and start sending it if its
        periodic and the tester is running.

        >>> tester.enable('PeriodicMessage1')

        """

        message = self._messages[message_name]
        message.enabled = True

        if self._is_running and message.periodic:
            message.send_periodic_start()

    def disable(self, message_name):
        """Disable given message `message_name` and stop sending it if its
        periodic, enabled and the tester is running.

        >>> tester.disable('PeriodicMessage1')

        """

        message = self._messages[message_name]
        message.enabled = False

        if self._is_running and message.periodic:
            message.send_periodic_stop()

    def send(self, message_name, signals=None):
        """Send given message `message_name` and optional signals `signals`.

        >>> tester.send('Message1', {'Signal2': 10})
        >>> tester.send('Message1')

        """

        self._messages[message_name].send(signals)

    def expect(self,
               message_name,
               signals=None,
               timeout=None,
               discard_other_messages=True):
        """Expect given message `message_name` and signal values `signals`
        within `timeout` seconds.

        Give `signals` as ``None`` to expect any signal values.

        Give `timeout` as ``None`` to wait forever.

        Messages are read from the input queue, and those not matching
        given `message_name` and `signals` are discarded if
        `discard_other_messages` is
        ``True``. :meth:`~cantools.tester.Tester.flush_input()` may be
        called to discard all old messages in the input queue before
        calling the expect function.

        Returns the expected message, or ``None`` on timeout.

        >>> tester.expect('Message2', {'Signal1': 13})
        {'Signal1': 13, 'Signal2': 9}

        """

        return self._messages[message_name].expect(signals,
                                                   timeout,
                                                   discard_other_messages)

    def flush_input(self):
        """Flush, or discard, all messages in the input queue.

        """

        del self._input_list[:]

        while not self._input_queue.empty():
            self._input_queue.get()

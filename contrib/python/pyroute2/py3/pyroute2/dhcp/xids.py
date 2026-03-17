from secrets import SystemRandom
from typing import Any, Optional, Union

from pyroute2.dhcp.fsm import State

random = SystemRandom()


def random_xid_prefix() -> int:
    '''A (max) 32 bit random int with its lowest nibble set to 0.

    These last 4 bits are used by the client to store its state, in the form
    of the associated `State` value, when sending a DHCP message.
    '''
    return random.randint(0x00000010, 0xFFFFFFF0)


class Xid:
    '''Transaction IDs used to identify responses to DHCP requests.

    We use the last nibble to store the state the message was sent in.

    The most significant bits store a random value we use to match requests
    and responses, since the RFC specifies that the server must send back
    the same value when answering. (see RFC section 4.1)
    '''

    def __init__(self, value: Optional[Union[int, 'Xid']] = None):
        if isinstance(value, int):
            int_value = value
        elif isinstance(value, Xid):
            int_value = int(value)
        elif value is None:
            int_value = random_xid_prefix()
        else:
            raise TypeError(f'{value!r} is not an xid')
        assert int_value < 0xFFFFFFFF  # we have 32 bits
        self._value = int_value

    @property
    def random_part(self) -> int:
        '''The random part of the transaction id.'''
        return self._value & 0xFFFFFFF0

    @property
    def request_state(self) -> Optional[State]:
        '''The state in which the request was sent.

        Since servers answer with the same transaction ID as the request,
        we can use this to know what client state does a response answer to.
        '''
        try:
            return State(self._value & 0xF)
        except ValueError:
            return None

    def for_state(self, state: State) -> 'Xid':
        '''A new Xid built from the random part + the state.'''
        return Xid(self.random_part | state)

    def __index__(self) -> int:
        '''Allows xids to be used as int.'''
        return self._value

    def matches(self, received_xid: 'Xid'):
        '''Loose match, whether the random part of both XIDs match.

        This can be used to check if a message is indeed in response
        to a request we sent.
        '''
        return self.random_part == received_xid.random_part

    def __eq__(self, other: Any) -> bool:
        '''Xids compare to other xids or ints.'''
        if isinstance(other, Xid):
            return self._value == other._value
        elif isinstance(other, int):
            return self._value == other
        return False

    def __str__(self) -> str:
        return hex(self._value)

    def __repr__(self) -> str:
        return f"Xid({self})"

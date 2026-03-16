"""Helper functions to build dhcp client messages."""

from dataclasses import dataclass
from typing import Iterable, Literal, Optional

from pyroute2.dhcp import enums
from pyroute2.dhcp.dhcp4msg import dhcp4msg
from pyroute2.dhcp.fsm import State
from pyroute2.dhcp.leases import Lease
from pyroute2.dhcp.xids import Xid

Parameters = Iterable[enums.dhcp.Option]


@dataclass
class _DHCPMessage:
    '''A DHCP message with some extra info from other layers.'''

    dhcp: dhcp4msg
    eth_src: str
    eth_dst: str = 'ff:ff:ff:ff:ff:ff'
    ip_src: str = '0.0.0.0'
    ip_dst: str = '255.255.255.255'
    sport: int = 68
    dport: int = 67

    @property
    def message_type(self) -> enums.dhcp.MessageType:
        '''The DHCP message type (DISCOVER, REQUEST, ACK...)'''
        return self.dhcp['options']['message_type']

    @property
    def xid(self) -> Xid:
        return Xid(self.dhcp['xid'])


@dataclass
class SentDHCPMessage(_DHCPMessage):
    '''A DHCP message to be sent to a server or broadcast.'''

    eth_src: Optional[str] = None  # type: ignore[assignment]

    def __str__(self) -> str:
        type_name = self.message_type.name
        return (
            f'{type_name} to {self.eth_dst}/{self.ip_dst}:{self.dport} '
            f'(xid {self.xid})'
        )


class ReceivedDHCPMessage(_DHCPMessage):
    '''A DHCP message received by the client.'''

    def __str__(self) -> str:
        type_name = self.dhcp['options']['message_type'].name
        return (
            f'{type_name} from {self.eth_src}/{self.ip_src}:{self.sport} '
            f'(xid {self.xid})'
        )


def discover(parameter_list: Parameters) -> SentDHCPMessage:
    '''Make a broadcast DISCOVER message for the given parameters.'''
    # Default for SentDHCPMessage is broadcast which is what we want here
    return SentDHCPMessage(
        dhcp=dhcp4msg(
            {
                'op': enums.bootp.MessageType.BOOTREQUEST,
                'flags': enums.bootp.Flag.BROADCAST,
                'options': {
                    'message_type': enums.dhcp.MessageType.DISCOVER,
                    'parameter_list': list(parameter_list),
                },
            }
        )
    )


def request_for_offer(
    parameter_list: Parameters, offer: ReceivedDHCPMessage
) -> SentDHCPMessage:
    '''Make a REQUEST message for a given OFFER.

    Since we don't have an IP yet, the message is always broadcast.
    When requesting an offer in the Selecting state, the server_id DHCP option
    is always set as opposed to when a REQUEST is sent in other states.

    See RFC 2131 section 4.3.2.
    '''
    return SentDHCPMessage(
        dhcp=dhcp4msg(
            {
                'op': enums.bootp.MessageType.BOOTREQUEST,
                'flags': enums.bootp.Flag.BROADCAST,
                'options': {
                    'message_type': enums.dhcp.MessageType.REQUEST,
                    'requested_ip': offer.dhcp['yiaddr'],
                    'server_id': offer.dhcp['options']['server_id'],
                    'parameter_list': list(parameter_list),
                },
            }
        )
    )


def request_for_lease(
    parameter_list: Parameters,
    lease: Lease,
    state: Literal[State.RENEWING, State.REBINDING, State.REBOOTING],
) -> SentDHCPMessage:
    '''Make a REQUEST for an existing lease.

    This differs from REQUESTs in response to an OFFER in that the server_id
    option is never set.

    When rebooting, the message is broadcast, and the requested_ip option is
    set to the IP in the stored lease. The bootp client IP is left blank.

    When renewing, (i.e. T1 expires) the message is for the server that granted
    the lease. The lease's IP is expected to be assigned to the client's
    interface at this point.

    When rebinding (T2), the message is broadcast on the network.

    In both cases, the bootp client IP (ciaddr) is set to the lease's IP.

    See RFC 2131 section 4.3.6.
    '''
    dhcp_msg = dhcp4msg(
        {
            'op': enums.bootp.MessageType.BOOTREQUEST,
            'flags': enums.bootp.Flag.BROADCAST,
            'options': {
                'message_type': enums.dhcp.MessageType.REQUEST,
                'parameter_list': list(parameter_list),
            },
        }
    )
    if state == State.REBOOTING:
        dhcp_msg['options']['requested_ip'] = lease.ip
    else:
        dhcp_msg['ciaddr'] = lease.ip
        if state == State.RENEWING:
            # T1 timer expired, send a request directly to the known server
            dhcp_msg['flags'] = enums.bootp.Flag.UNICAST
            return SentDHCPMessage(
                dhcp=dhcp_msg,
                eth_dst=lease.server_mac,
                # default ip_dst to broadcast, it should not be necessary
                # but it'll avoid a crash if the server_id is missing
                ip_dst=lease.server_id or '255.255.255.255',
                ip_src=lease.ip,
            )
    # Reboot or rebind, broadcast the request
    return SentDHCPMessage(dhcp=dhcp_msg)


def release(lease: Lease) -> SentDHCPMessage:
    '''Make a RELEASE for an existing & active lease.'''
    # RELEASE messages have nearly no allowed options,
    # and the released IP address must be set in ciaddr
    return SentDHCPMessage(
        dhcp=dhcp4msg(
            {
                'op': enums.bootp.MessageType.BOOTREQUEST,
                'flags': enums.bootp.Flag.UNICAST,
                'ciaddr': lease.ip,
                'options': {
                    'message_type': enums.dhcp.MessageType.RELEASE,
                    'server_id': lease.server_id,
                },
            }
        ),
        # RELEASEs are unicast (see rfc section 4.4.4)
        eth_dst=lease.server_mac,
        # default ip_dst to broadcast, it should not be necessary
        # but it'll avoid a crash if the server_id is missing
        ip_dst=lease.server_id or '255.255.255.255',
        ip_src=lease.ip,
    )

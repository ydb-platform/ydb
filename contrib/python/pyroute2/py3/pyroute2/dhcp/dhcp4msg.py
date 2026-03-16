from socket import AF_INET, inet_ntop, inet_pton

from pyroute2.dhcp import Policy, dhcpmsg, option
from pyroute2.dhcp.enums.dhcp import Option


class dhcp4msg(dhcpmsg):
    #
    # https://www.ietf.org/rfc/rfc2131.txt
    #
    fields = (
        ('op', 'uint8', 1),  # request
        ('htype', 'uint8', 1),  # ethernet
        ('hlen', 'uint8', 6),  # ethernet addr len
        ('hops', 'uint8'),
        ('xid', 'be32'),
        ('secs', 'be16'),
        ('flags', 'be16'),
        ('ciaddr', 'ip4addr'),
        ('yiaddr', 'ip4addr'),
        ('siaddr', 'ip4addr'),
        ('giaddr', 'ip4addr'),
        ('chaddr', 'l2paddr'),
        ('sname', '64s'),
        ('file', '128s'),
        ('cookie', '4s', b'c\x82Sc'),
    )
    #
    # https://www.ietf.org/rfc/rfc2132.txt
    #
    options = (
        # TODO: add & test more options
        (Option.PAD, 'none'),
        (Option.SUBNET_MASK, 'ip4addr'),
        (Option.TIME_OFFSET, 'be32'),
        (Option.ROUTER, 'ip4list'),
        (Option.TIME_SERVER, 'ip4list'),
        (Option.IEN_NAME_SERVER, 'ip4list'),
        (Option.NAME_SERVER, 'ip4list'),
        (Option.LOG_SERVER, 'ip4list'),
        (Option.COOKIE_SERVER, 'ip4list'),
        (Option.LPR_SERVER, 'ip4list'),
        (Option.IMPRESS_SERVER, 'ip4list'),
        (Option.RESOURCE_LOCATION_SERVER, 'ip4list'),
        (Option.HOST_NAME, 'string'),
        # in multiples of 512 bytes
        (Option.BOOT_FILE_SIZE, 'be16'),
        (Option.MERIT_DUMP_FILE, 'string'),
        (Option.DOMAIN_NAME, 'string'),
        (Option.SWAP_SERVER, 'ip4addr'),
        (Option.ROOT_PATH, 'string'),
        (Option.EXTENSIONS_PATH, 'string'),
        (Option.IP_FORWARDING, 'bool'),
        (Option.NON_LOCAL_SOURCE_ROUTING, 'bool'),
        # TODO: dict of ip/mask pairs
        # (Option.POLICY_FILTER, '?'),
        # minimum legal value is 567 according to rfc
        (Option.MAX_DATAGRAM_REASSEMBLY, 'be16'),
        (Option.DEFAULT_TTL, 'uint8'),
        (Option.PATH_MTU_AGING_TIMEOUT, 'be32'),
        # TODO: list of be16
        # (Option.PATH_MTU_PLATEAU_TABLE, '?'),
        (Option.INTERFACE_MTU, 'be16'),
        (Option.ALL_SUBNETS_LOCAL, 'bool'),
        (Option.BROADCAST_ADDRESS, 'ip4addr'),
        (Option.PERFORM_MASK_DISCOVERY, 'bool'),
        (Option.MASK_SUPPLIER, 'bool'),
        (Option.PERFORM_ROUTER_DISCOVERY, 'bool'),
        (Option.ROUTER_SOLICITATION_ADDRESS, 'ip4addr'),
        # TODO: dict of ip addrs
        # (Option.STATIC_ROUTE, '?')
        (Option.TRAILER_ENCAPSULATION, 'bool'),
        (Option.ARP_CACHE_TIMEOUT, 'be32'),
        (Option.ETHERNET_ENCAPSULATION, 'bool'),
        (Option.TCP_DEFAULT_TTL, 'uint8'),
        (Option.TCP_KEEPALIVE_INTERVAL, 'be32'),
        (Option.TCP_KEEPALIVE_GARBAGE, 'bool'),
        (Option.NIS_DOMAIN, 'string'),
        (Option.NDS_SERVERS, 'ip4list'),
        (Option.NTP_SERVERS, 'ip4list'),
        (Option.VENDOR_SPECIFIC_INFORMATION, 'string'),
        (Option.NETBIOS_NAME_SERVER, 'ip4list'),
        (Option.NETBIOS_DDG_SERVER, 'ip4list'),
        # 1=B, 2=P, 4=M, 8=H, probably obsolete & unused
        (Option.NETBIOS_NODE_TYPE, 'uint8'),
        (Option.NETBIOS_SCOPE, 'string'),
        (Option.X_WINDOW_FONT_SERVER, 'ip4list'),
        (Option.X_WINDOW_DISPLAY_MANAGER, 'ip4list'),
        (Option.REQUESTED_IP, 'ip4addr'),
        (Option.LEASE_TIME, 'sbe32'),
        # 1: options in file, 2: options in sname, 4, both
        (Option.OPTION_OVERLOAD, 'uint8'),
        (Option.TFTP_SERVER_NAME, 'string'),
        (Option.BOOTFILE_NAME, 'string'),
        (Option.MESSAGE_TYPE, 'message_type'),
        (Option.SERVER_ID, 'ip4addr'),
        (Option.PARAMETER_LIST, 'array8'),
        (Option.MESSAGE, 'string'),
        # minimum value: 576 bytes
        (Option.MAX_MSG_SIZE, 'be16'),
        (Option.RENEWAL_TIME, 'sbe32'),
        (Option.REBINDING_TIME, 'sbe32'),
        (Option.VENDOR_ID, 'string'),
        (Option.CLIENT_ID, 'client_id'),
        (Option.SUBNET_SELECTION, 'ip4addr'),
        (Option.END, 'none'),
    )

    class ip4addr(option):
        policy = Policy(
            format='4s',
            encode=lambda x: inet_pton(AF_INET, x),
            decode=lambda x: inet_ntop(AF_INET, x),
        )

    class ip4list(option):
        policy = Policy(
            format='string',
            encode=lambda x: b''.join([inet_pton(AF_INET, i) for i in x]),
            decode=lambda x: [
                inet_ntop(AF_INET, x[i * 4 : i * 4 + 4])
                for i in range(len(x) // 4)
            ],
        )

import os
import socket
import subprocess
from socket import AF_INET, AF_INET6

from pyroute2.common import dqn2int
from pyroute2.netlink import NLM_F_DUMP, NLM_F_MULTI, NLM_F_REQUEST, NLMSG_DONE
from pyroute2.netlink.rtnl import (
    RTM_GETADDR,
    RTM_GETLINK,
    RTM_GETNEIGH,
    RTM_GETROUTE,
    RTM_NEWADDR,
    RTM_NEWLINK,
    RTM_NEWNEIGH,
    RTM_NEWROUTE,
)
from pyroute2.netlink.rtnl.ifaddrmsg import ifaddrmsg
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.marshal import MarshalRtnl


class IPRoute(object):
    """
    macOS-specific IPRoute class (proof-of-concept).
    Attempts to fetch interfaces, addresses, neighbors, and routes
    by parsing macOS command output (ifconfig, netstat, arp, ndp, etc.).
    """

    def __init__(self, *argv, **kwarg):
        self.marshal = MarshalRtnl()
        self.target = kwarg.get('target') or 'localhost'
        self._outq = kwarg.get('_outq', None)
        self._pfdw = kwarg.get('_pfdw', None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def clone(self):
        return self

    def close(self, code=None):
        pass

    def bind(self, *argv, **kwarg):
        pass

    def getsockopt(self, *argv, **kwarg):
        return 1024 * 1024

    def sendto_gate(self, msg, addr):
        """
        Handle incoming netlink requests (simulation for macOS).
        """
        cmd = msg['header']['type']
        flags = msg['header']['flags']
        seq = msg['header']['sequence_number']

        # Only handle dump requests
        if flags != NLM_F_REQUEST | NLM_F_DUMP:
            return

        if cmd == RTM_GETLINK:
            rtype = RTM_NEWLINK
            ret = self.get_links()
        elif cmd == RTM_GETADDR:
            rtype = RTM_NEWADDR
            ret = self.get_addr()
        elif cmd == RTM_GETROUTE:
            rtype = RTM_NEWROUTE
            ret = self.get_routes()
        elif cmd == RTM_GETNEIGH:
            rtype = RTM_NEWNEIGH
            ret = self.get_neighbours()
        else:
            ret = []

        # Set response type and finalize
        for r in ret:
            r['header']['type'] = rtype
            r['header']['flags'] = NLM_F_MULTI
            r['header']['sequence_number'] = seq

        done_msg = type(msg)()
        done_msg['header']['type'] = NLMSG_DONE
        done_msg['header']['sequence_number'] = seq
        ret.append(done_msg)

        # If using an output queue (as done in Windows code),
        # write data there
        if self._outq and self._pfdw:
            data = b''
            for r in ret:
                r.encode()
                data += r.data
            self._outq.put(data)
            os.write(self._pfdw, b'\0')

    def _parse_ifconfig(self):
        """
        Parse `ifconfig` output to gather interface info, MAC, and addresses.
        Returns a dictionary:
        {
          'interfaces': [ifinfmsg, ifinfmsg, ...],
          'addresses':  [ifaddrmsg, ifaddrmsg, ...]
        }
        """
        ret = {'interfaces': [], 'addresses': []}

        try:
            output = subprocess.check_output(["ifconfig"]).decode(
                "utf-8", errors="replace"
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return ret

        blocks = output.strip().split("\n\n")

        for block in blocks:
            lines = block.strip().splitlines()
            if not lines:
                continue

            # The first line usually has "en0: flags=..."
            first_line = lines[0]
            iface_name = first_line.split(":")[0].strip()

            # Attempt to fetch interface index and MAC address
            try:
                iface_index = socket.if_nametoindex(iface_name)
            except OSError:
                iface_index = 0  # fallback if something goes wrong

            mac_addr = "00:00:00:00:00:00"
            for line in lines:
                # Lines with "ether " on macOS contain MAC, e.g.:
                # "ether 00:1c:42:aa:bb:cc"
                if line.strip().startswith("ether "):
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        mac_addr = parts[1]
                    break

            # Build ifinfmsg
            spec_if = {
                'index': iface_index,
                'attrs': (
                    ['IFLA_ADDRESS', mac_addr],
                    ['IFLA_IFNAME', iface_name],
                ),
            }
            msg_if = ifinfmsg().load(spec_if)
            msg_if['header']['target'] = self.target
            msg_if['header']['type'] = RTM_NEWLINK
            # remove 'value' if present
            msg_if.pop('value', None)
            ret['interfaces'].append(msg_if)

            # Look for IPv4 or IPv6 addresses
            # "inet 192.168.1.10 netmask 0xffffff00 broadcast 192.168.1.255"
            # "inet6 fe80::... prefixlen 64 ..."
            for line in lines[1:]:
                parts = line.strip().split()
                if line.strip().startswith("inet "):
                    # IPv4
                    try:
                        ip_idx = parts.index("inet") + 1
                        ip_addr = parts[ip_idx]
                        mask_str = "255.255.255.0"  # fallback
                        if "netmask" in parts:
                            mask_idx = parts.index("netmask") + 1
                            mask_hex = parts[mask_idx]
                            if mask_hex.startswith("0x"):
                                nm_value = int(mask_hex, 16)
                                dotted_mask = []
                                for _ in range(4):
                                    dotted_mask.insert(0, str(nm_value & 0xFF))
                                    nm_value >>= 8
                                mask_str = ".".join(dotted_mask)

                        prefix_len = dqn2int(mask_str)

                        spec_addr = {
                            'index': iface_index,
                            'family': AF_INET,
                            'prefixlen': prefix_len,
                            'attrs': (
                                ['IFA_ADDRESS', ip_addr],
                                ['IFA_LOCAL', ip_addr],
                                ['IFA_LABEL', iface_name],
                            ),
                        }
                        msg_addr = ifaddrmsg().load(spec_addr)
                        msg_addr['header']['target'] = self.target
                        msg_addr['header']['type'] = RTM_NEWADDR
                        msg_addr.pop('value', None)
                        ret['addresses'].append(msg_addr)
                    except (ValueError, IndexError):
                        continue
                elif line.strip().startswith("inet6 "):
                    # IPv6
                    try:
                        ip_idx = parts.index("inet6") + 1
                        ip_addr = parts[ip_idx]
                        prefix_len = 64  # default if not found

                        # On macOS you'll often see
                        # "inet6 fe80::xxxx prefixlen 64 ..."
                        if "prefixlen" in parts:
                            pre_idx = parts.index("prefixlen") + 1
                            prefix_len = int(parts[pre_idx])

                        spec_addr = {
                            'index': iface_index,
                            'family': AF_INET6,
                            'prefixlen': prefix_len,
                            'attrs': (
                                ['IFA_ADDRESS', ip_addr],
                                ['IFA_LOCAL', ip_addr],
                                ['IFA_LABEL', iface_name],
                            ),
                        }
                        msg_addr = ifaddrmsg().load(spec_addr)
                        msg_addr['header']['target'] = self.target
                        msg_addr['header']['type'] = RTM_NEWADDR
                        msg_addr.pop('value', None)
                        ret['addresses'].append(msg_addr)
                    except (ValueError, IndexError):
                        continue

        return ret

    def _parse_routes(self):
        """
        Parse `netstat -rn` to retrieve routing table entries.
        Return list of ifinfmsg-like route objects, though strictly
        they'd be `rtmsg` in a real Netlink environment.
        For demonstration, we just return RTM_NEWROUTE messages
        with minimal info: destination, gateway, interface index, etc.
        """
        routes = []
        try:
            output = subprocess.check_output(["netstat", "-rn"]).decode(
                "utf-8", errors="replace"
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return routes

        # On macOS, `netstat -rn` has headers like:
        # Destination        Gateway            Flags        Netif Expire
        # default            192.168.1.1        UGSc         en0
        # 127                127.0.0.1          UCS          lo0
        # ...
        lines = output.strip().splitlines()
        # skip header lines
        # find the header first
        start_index = 0
        for i, line in enumerate(lines):
            if "Destination" in line and "Gateway" in line:
                start_index = i + 1
                break

        # parse from start_index onward
        for line in lines[start_index:]:
            parts = line.split()
            if len(parts) < 4:
                continue
            destination, gateway, flags, netif = parts[:4]
            # Attempt to get interface index
            try:
                idx = socket.if_nametoindex(netif)
            except OSError:
                idx = 0

            # We'll store the route as ifinfmsg for
            # demonstration, but real netlink
            # code would use `rtmsg`. We'll keep it
            # consistent with the rest of this
            # POC approach.
            spec_route = {
                'index': idx,
                # not strictly correct, but placing
                # them in attrs for demonstration
                'attrs': (
                    ['ROUTE_DST', destination],
                    ['ROUTE_GATEWAY', gateway],
                    ['ROUTE_FLAGS', flags],
                    ['ROUTE_IFNAME', netif],
                ),
            }
            # We'll build it as ifinfmsg with a 'type' = RTM_NEWROUTE
            msg_route = ifinfmsg().load(spec_route)
            msg_route['header']['target'] = self.target
            msg_route['header']['type'] = RTM_NEWROUTE
            msg_route.pop('value', None)
            routes.append(msg_route)

        return routes

    def _parse_arp_neighbors(self):
        """
        Parse `arp -an` for IPv4 neighbors and (optionally)
        `ndp -an` for IPv6.
        Returns a list of ifinfmsg (or ifaddrmsg)
        objects simulating neighbor entries.
        """
        neighbors = []
        # IPv4 neighbors (ARP)
        try:
            output = subprocess.check_output(["arp", "-an"]).decode(
                "utf-8", errors="replace"
            )
            # Lines look like:
            # "? (192.168.1.10) at 00:1c:42:xx:yy:zz on en0 ifscope [ethernet]"
            for line in output.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                # Quick parse:
                # 1) IP in parentheses
                # 2) MAC after "at"
                # 3) interface name after "on"
                parts = line.split()
                # example parts:
                # ["?", "(192.168.1.10)",
                # "at", "00:1c:42:xx:yy:zz",
                # "on", "en0", ...]
                if len(parts) < 7:
                    continue
                ip_str = parts[1].strip("()")
                mac_str = parts[3]
                iface_str = parts[5]
                try:
                    idx = socket.if_nametoindex(iface_str)
                except OSError:
                    idx = 0

                spec_neigh = {
                    'index': idx,
                    'attrs': (
                        ['NEIGH_IP', ip_str],
                        ['NEIGH_LLADDR', mac_str],
                        ['NEIGH_IFNAME', iface_str],
                    ),
                }
                msg_neigh = ifinfmsg().load(spec_neigh)
                msg_neigh['header']['target'] = self.target
                msg_neigh['header']['type'] = RTM_NEWNEIGH
                msg_neigh.pop('value', None)
                neighbors.append(msg_neigh)
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

        # Optional: IPv6 neighbors (NDP)
        try:
            output = subprocess.check_output(["ndp", "-an"]).decode(
                "utf-8", errors="replace"
            )
            # Lines look like:
            # "fe80::1%lo0            lladdr 00:00:00:...  router  STALE"
            for line in output.strip().splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) < 4:
                    continue
                # typical parts:
                # ["fe80::1%lo0",
                # "lladdr", "00:00:00:..",
                # "router", "STALE"]
                # or ["2001:db8::1234%en0",
                # "lladdr",
                # "00:11:22:33:44:55",
                # "REACHABLE"]
                addr_part = parts[0]
                # separate out the interface from the IP
                if "%" in addr_part:
                    ip_str, iface_str = addr_part.split("%", 1)
                else:
                    ip_str = addr_part
                    iface_str = "??"
                mac_str = None
                if "lladdr" in parts:
                    ll_idx = parts.index("lladdr") + 1
                    if ll_idx < len(parts):
                        mac_str = parts[ll_idx]
                if not mac_str:
                    continue
                try:
                    idx = socket.if_nametoindex(iface_str)
                except OSError:
                    idx = 0

                spec_neigh = {
                    'index': idx,
                    'attrs': (
                        ['NEIGH_IP', ip_str],
                        ['NEIGH_LLADDR', mac_str],
                        ['NEIGH_IFNAME', iface_str],
                    ),
                }
                msg_neigh = ifinfmsg().load(spec_neigh)
                msg_neigh['header']['target'] = self.target
                msg_neigh['header']['type'] = RTM_NEWNEIGH
                msg_neigh.pop('value', None)
                neighbors.append(msg_neigh)
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

        return neighbors

    def dump(self, groups=None):
        # Return all info in sequence
        for method in (
            self.get_links,
            self.get_addr,
            self.get_neighbours,
            self.get_routes,
        ):
            for msg in method():
                yield msg

    def get_links(self, *argv, **kwarg):
        '''
        {
          'attrs': (['IFLA_ADDRESS', '9a:9d:81:90:d2:8f'],
                    ['IFLA_IFNAME', 'lo0']),
          'header': {'target': 'localhost', 'type': 16},
          'index': 1
        }
        '''
        return self._parse_ifconfig()['interfaces']

    def get_addr(self, *argv, **kwarg):
        '''
        {
          'attrs': (['IFA_ADDRESS', '127.0.0.1'],
                    ['IFA_LOCAL', '127.0.0.1'],
                    ['IFA_LABEL', 'lo0']),
          'header': {'target': 'localhost', 'type': 20},
          'index': 1,
          'family': <AddressFamily.AF_INET: 2>,
          'prefixlen': 8
        }
        '''
        return self._parse_ifconfig()['addresses']

    def get_neighbours(self, *argv, **kwarg):
        return self._parse_arp_neighbors()

    def get_routes(self, *argv, **kwarg):
        return self._parse_routes()


class RawIPRoute(IPRoute):
    pass


class ChaoticIPRoute:
    """
    Placeholder, mirroring its Windows counterpart.
    """

    def __init__(self, *argv, **kwarg):
        raise NotImplementedError()


class NetNS:
    """
    Another placeholder class to match the Windows code structure.
    """

    def __init__(self, *argv, **kwarg):
        raise NotImplementedError()

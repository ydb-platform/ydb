import urllib.parse


def collect_hosts(hosts):
    """
    Collect a set of hosts and an optional chroot from
    a string or a list of strings.
    """
    if isinstance(hosts, list):
        if hosts[-1].strip().startswith("/"):
            host_ports, chroot = hosts[:-1], hosts[-1]
        else:
            host_ports, chroot = hosts, None
    else:
        host_ports, chroot = hosts.partition("/")[::2]
        host_ports = host_ports.split(",")
        chroot = "/" + chroot if chroot else None

    result = []
    for host_port in host_ports:
        # put all complexity of dealing with
        # IPv4 & IPv6 address:port on the urlsplit
        res = urllib.parse.urlsplit("xxx://" + host_port)
        host = res.hostname
        if host is None:
            raise ValueError("bad hostname")
        port = int(res.port) if res.port else 2181
        result.append((host.strip(), port))

    return result, chroot

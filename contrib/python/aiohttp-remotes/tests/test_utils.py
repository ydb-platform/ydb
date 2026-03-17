from ipaddress import IPv4Address, IPv6Address, ip_address, ip_network

import pytest

from aiohttp_remotes.exceptions import IncorrectIPCount, UntrustedIP
from aiohttp_remotes.utils import parse_trusted_list, remote_ip


def test_parse_str() -> None:
    with pytest.raises(TypeError):
        parse_trusted_list("127.0.0.1")


def test_parse_non_sequence() -> None:
    with pytest.raises(TypeError):
        parse_trusted_list(1)  # type: ignore[arg-type]


def test_parse_non_sequence_of_containers() -> None:
    with pytest.raises(TypeError):
        parse_trusted_list([1])  # type: ignore


def test_parse_ipv4() -> None:
    ret = parse_trusted_list([[IPv4Address("127.0.0.1")]])
    assert ret == [[IPv4Address("127.0.0.1")]]


def test_parse_ipv6() -> None:
    ret = parse_trusted_list([[IPv6Address("::1")]])
    assert ret == [[IPv6Address("::1")]]


def test_parse_ipv4_str() -> None:
    ret = parse_trusted_list([["127.0.0.1"]])
    assert ret == [[IPv4Address("127.0.0.1")]]


def test_parse_ipv6_str() -> None:
    ret = parse_trusted_list([["::1"]])
    assert ret == [[IPv6Address("::1")]]


def test_parse_non_ip_item() -> None:
    with pytest.raises(ValueError):
        parse_trusted_list([["garbage"]])


def test_parse_ellipsis_at_beginning() -> None:
    ret = parse_trusted_list([["127.0.0.1"], ...])
    assert ret == [[IPv4Address("127.0.0.1")], ...]


def test_parse_ellipsis_after_address() -> None:
    with pytest.raises(ValueError):
        parse_trusted_list([..., ["127.0.0.1"]])


# --------------------- remote_ip -----------------------


def test_remote_ip_no_trusted() -> None:
    ip = ip_address("10.10.10.10")
    assert ip == remote_ip([], [ip])


def test_remote_ip_ok() -> None:
    ips = [
        ip_address("10.10.10.10"),
        ip_address("20.20.20.20"),
        ip_address("30.30.30.30"),
    ]
    trusted = parse_trusted_list([["10.10.0.0/16"], ["20.20.20.20"]])
    assert ips[-1] == remote_ip(trusted, ips)


def test_remote_ip_not_trusted_network() -> None:
    ips = [
        ip_address("10.10.10.10"),
        ip_address("20.20.20.20"),
        ip_address("30.30.30.30"),
    ]
    trusted = parse_trusted_list([["40.40.0.0/16"], ["20.20.20.20"]])
    with pytest.raises(UntrustedIP) as ctx:
        remote_ip(trusted, ips)
    assert list(ctx.value.trusted) == [ip_network("40.40.0.0/16")]
    assert ctx.value.ip == ip_address("10.10.10.10")


def test_remote_ip_not_trusted_ip() -> None:
    ips = [
        ip_address("10.10.10.10"),
        ip_address("20.20.20.20"),
        ip_address("30.30.30.30"),
    ]
    trusted = parse_trusted_list([["40.40.40.40"], ["20.20.20.20"]])
    with pytest.raises(UntrustedIP) as ctx:
        remote_ip(trusted, ips)
    assert ctx.value.trusted == [ip_address("40.40.40.40")]
    assert ctx.value.ip == ip_address("10.10.10.10")


def test_remote_ip_invalis_ips_count() -> None:
    ips = [ip_address("10.10.10.10"), ip_address("20.20.20.20")]
    trusted = parse_trusted_list([["40.40.40.40"], ["20.20.20.20"]])
    with pytest.raises(IncorrectIPCount) as ctx:
        remote_ip(trusted, ips)
    assert ctx.value.expected == 3
    assert ctx.value.actual == [IPv4Address("10.10.10.10"), IPv4Address("20.20.20.20")]


def test_remote_with_ellipsis() -> None:
    ips = [
        ip_address("10.10.10.10"),
        ip_address("20.20.20.20"),
        ip_address("30.30.30.30"),
    ]
    trusted = parse_trusted_list([["10.10.0.0/16"], ...])
    assert ips[-2] == remote_ip(trusted, ips)

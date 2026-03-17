PY3TEST()

SRCDIR(
    contrib/python/netaddr/py3/netaddr/tests
)

TEST_SRCS(
    __init__.py
    core/__init__.py
    core/test_compat.py
    core/test_pubsub.py
    eui/__init__.py
    eui/test_eui.py
    eui/test_ieee_parsers.py
    ip/__init__.py
    ip/test_cidr_v4.py
    ip/test_cidr_v6.py
    ip/test_dns.py
    ip/test_ip.py
    ip/test_ip_categories.py
    ip/test_ip_comparisons.py
    ip/test_ip_globs.py
    ip/test_ip_ranges.py
    ip/test_ip_rfc1924.py
    ip/test_ip_sets.py
    ip/test_ip_splitter.py
    ip/test_ip_v4.py
    ip/test_ip_v4_v6_conversions.py
    ip/test_ip_v6.py
    ip/test_network_ops.py
    ip/test_nmap.py
    ip/test_old_specs.py
    ip/test_platform_osx.py
    ip/test_socket_module_fallback.py
    strategy/__init__.py
    strategy/test_eui48_strategy.py
    strategy/test_ipv4_strategy.py
    strategy/test_ipv6_strategy.py
    test_netaddr.py
)

RESOURCE_FILES(
    PREFIX contrib/python/netaddr/py3/netaddr/tests/
    eui/sample_iab.txt
    eui/sample_oui.txt
)

PEERDIR(
    contrib/python/netaddr
)

FORK_TESTS()

NO_LINT()

END()

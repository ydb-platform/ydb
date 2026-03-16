from typing import Optional, Union

import pytest

from pyscopg2.utils import Dsn, host_is_ipv6_address, split_dsn


FORMAT_DSN_TEST_CASES = [
    ["localhost", 5432, None, None, None, "postgresql://localhost:5432"],
    ["localhost", 5432, "user", None, None, "postgresql://user@localhost:5432"],
    ["localhost", 5432, "user", "pwd", None, "postgresql://user:pwd@localhost:5432"],
    ["localhost", 5432, None, None, "testdb", "postgresql://localhost:5432/testdb"],
    [
        "localhost",
        "5432",
        "user",
        None,
        "testdb",
        "postgresql://user@localhost:5432/testdb",
    ],
    [
        "localhost",
        5432,
        "user",
        "pwd",
        "testdb",
        "postgresql://user:pwd@localhost:5432/testdb",
    ],
]


@pytest.mark.parametrize(
    ["host", "port", "user", "password", "dbname", "expected_result"],
    FORMAT_DSN_TEST_CASES,
)
def test_format_dsn(
    host: str,
    port: Union[str, int],
    user: Optional[str],
    password: Optional[str],
    dbname: Optional[str],
    expected_result: str,
):
    result_dsn = Dsn(host=host, port=port, user=user, password=password, dbname=dbname)
    assert str(result_dsn) == expected_result


def test_replace_dsn_params():
    dsn = Dsn(
        host="localhost", port=5432, user="user", password="password", dbname="testdb",
    )
    replaced_dsn = dsn.with_(password="***")
    assert str(replaced_dsn) == "postgresql://user:***@localhost:5432/testdb"


def test_split_single_host_dsn():
    source_dsn = "postgresql://user:pwd@localhost:5432/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == source_dsn


def test_split_single_host_dsn_without_port():
    source_dsn = "postgresql://user:pwd@localhost/testdb"
    result_dsn = split_dsn(source_dsn, default_port=1)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == "postgresql://user:pwd@localhost:1/testdb"


def test_split_multi_host_dsn():
    source_dsn = "postgresql://user:pwd@master:5432,replica:5432,replica:6432/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 3
    master_dsn, fst_replica_dsn, snd_replica_dsn = result_dsn
    assert str(master_dsn) == "postgresql://user:pwd@master:5432/testdb"
    assert str(fst_replica_dsn) == "postgresql://user:pwd@replica:5432/testdb"
    assert str(snd_replica_dsn) == "postgresql://user:pwd@replica:6432/testdb"


def test_split_dsn_skip_same_addreses():
    source_dsn = "postgresql://user:pwd@localhost:5432,localhost:5432/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == "postgresql://user:pwd@localhost:5432/testdb"


def test_split_dsn_with_default_port():
    source_dsn = "postgresql://user:pwd@master:6432,replica/testdb"
    result_dsn = split_dsn(source_dsn, default_port=15432)
    assert len(result_dsn) == 2
    master_dsn, replica_dsn = result_dsn
    assert str(master_dsn) == "postgresql://user:pwd@master:6432/testdb"
    assert str(replica_dsn) == "postgresql://user:pwd@replica:15432/testdb"


@pytest.mark.parametrize(
    ["hosts_count"],
    [
        [1024],
    ],
)
def test_split_large_dsn(hosts_count: int):
    hosts = [f"host-{i}" for i in range(hosts_count)]
    large_dsn = "postgresql://user:pwd@" + ",".join(hosts) + "/testdb"
    result_dsn = split_dsn(large_dsn, default_port=5432)
    for i, dsn in enumerate(result_dsn):
        assert str(dsn) == f"postgresql://user:pwd@host-{i}:5432/testdb"


def test_split_dsn_with_params():
    dsn = (
        "postgresql://user:password@master:5432,replica:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    expected_master_dsn = (
        "postgresql://user:password@master:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    expected_replica_dsn = (
        "postgresql://user:password@replica:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    master_dsn, replica_dsn = split_dsn(dsn)
    assert str(master_dsn) == expected_master_dsn
    assert str(replica_dsn) == expected_replica_dsn


def test_replace_dsn_part():
    dsn = "postgresql://user:password@localhost:5432/testdb"
    expected_dsn = "postgresql://user:***@localhost:5432/testdb"
    result_dsn, *_ = split_dsn(dsn)
    dsn_with_hidden_password = result_dsn.with_(password="***")
    assert str(dsn_with_hidden_password) == expected_dsn


@pytest.mark.parametrize(
    ["host", "expected_result"],
    [
        ["yandex.ru", False],
        ["127.0.0.1", False],
        ["2001:DB8:3C4D:7777:260:3EFF:FE15:9501", True],
        ["2001:dead:beef::1", True],
    ],
)
def test_host_is_ipv6_address(host: str, expected_result: bool):
    result = host_is_ipv6_address(host)
    assert result == expected_result


def test_ipv6_host_in_dsn():
    dsn = "postgresql://user:password@[2001:DB8:3C4D:7777:260:3EFF:FE15:9501]:5432/testdb"
    result_dsn, *_ = split_dsn(dsn)
    assert str(result_dsn) == dsn

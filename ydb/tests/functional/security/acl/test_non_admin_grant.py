# -*- coding: utf-8 -*-
import contextlib
import logging
import uuid

import pytest

from ydb.tests.functional.security.lib.cluster_config import create_ydb_configurator, generate_certificates
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

ADMIN_TOKEN = 'root@builtin'
DATABASE = '/Root/test'
TABLE_PATH = f'{DATABASE}/table'


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('acl_certs_')
    return generate_certificates(str(certs_tmp_dir))


@contextlib.contextmanager
def cluster_with_tenant_database(certificates, empty_administration_allowed_sids):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=False,
        empty_administration_allowed_sids=empty_administration_allowed_sids,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    try:
        cluster.create_database(DATABASE, storage_pool_units_count={'hdd': 1}, token=ADMIN_TOKEN)
        database_nodes = cluster.register_and_start_slots(DATABASE, count=1)
        cluster.wait_tenant_up(DATABASE, token=ADMIN_TOKEN)
        try:
            # an object of the admin, the user under test owns nothing
            run_with_assert(admin_config(cluster), f"CREATE TABLE `{TABLE_PATH}` (a Uint64, PRIMARY KEY (a));")
            yield cluster
        finally:
            cluster.remove_database(DATABASE, token=ADMIN_TOKEN)
            cluster.unregister_and_stop_slots(database_nodes)
    finally:
        cluster.stop()


@pytest.fixture
def cluster_without_cluster_admins(certificates):
    with cluster_with_tenant_database(certificates, empty_administration_allowed_sids=True) as cluster:
        yield cluster


@pytest.fixture
def cluster_with_cluster_admins(certificates):
    with cluster_with_tenant_database(certificates, empty_administration_allowed_sids=False) as cluster:
        yield cluster


def run_query(config, query):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(query)


def run_with_assert(config, query, expected_err=None):
    if not expected_err:
        run_query(config, query)
        return

    try:
        run_query(config, query)
        assert False, 'Error expected'
    except Exception as e:
        assert expected_err in str(e)


def unique_user_name(prefix):
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def admin_config(cluster):
    return ydb.DriverConfig(
        endpoint="%s:%s" % (cluster.nodes[1].host, cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.AuthTokenCredentials(ADMIN_TOKEN),
    )


def provide_grants(admin_driver_config, user_name, object_name, required_grants):
    grants = ", ".join(f"'{grant}'" for grant in required_grants)
    run_with_assert(admin_driver_config, f"GRANT {grants} ON `{object_name}` TO {user_name};")


def create_user_with_minimal_grants(cluster, admin_driver_config):
    user_name = unique_user_name("user")
    run_with_assert(admin_driver_config, f"CREATE USER {user_name};")
    # the least a user needs to reach the object, no 'ydb.access.grant' among them
    provide_grants(admin_driver_config, user_name, DATABASE, ['ydb.database.connect', 'ydb.granular.describe_schema'])
    user_driver_config = ydb.DriverConfig(
        endpoint="%s:%s" % (cluster.nodes[1].host, cluster.nodes[1].port),
        database=DATABASE,
        credentials=ydb.StaticCredentials.from_user_password(user_name, ""),
    )
    return user_name, user_driver_config


def self_grant_query(user_name):
    return f"GRANT 'ydb.granular.select_row' ON `{TABLE_PATH}` TO {user_name};"


def test_user_can_grant_itself_without_cluster_admins(cluster_without_cluster_admins):
    cluster = cluster_without_cluster_admins
    user_name, user_driver_config = create_user_with_minimal_grants(cluster, admin_config(cluster))

    # empty administration_allowed_sids makes any token an administrator,
    # so the user bypasses the grant access check on the admin's table
    run_with_assert(user_driver_config, self_grant_query(user_name))


def test_user_cannot_grant_itself_with_cluster_admins(cluster_with_cluster_admins):
    cluster = cluster_with_cluster_admins
    tenant_admin_config = admin_config(cluster)
    user_name, user_driver_config = create_user_with_minimal_grants(cluster, tenant_admin_config)

    run_with_assert(user_driver_config, self_grant_query(user_name), expected_err="Access denied")
    run_with_assert(tenant_admin_config, self_grant_query(user_name))

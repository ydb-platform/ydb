# -*- coding: utf-8 -*-
import logging

from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    extra_feature_flags=['enable_strict_acl_check']
)


def test_create_user(ydb_client, ydb_root, ydb_database):
    with ydb_client(ydb_root) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries("CREATE USER user;")

    with ydb_client(ydb_database) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            finished = False
            try:
                pool.execute_with_retries(f"GRANT ALL ON `{ydb_database}` TO user;")
                finished = True
            except Exception as e:
                assert f"SID user not found in database `{ydb_database}`" in str(e)

            assert not finished

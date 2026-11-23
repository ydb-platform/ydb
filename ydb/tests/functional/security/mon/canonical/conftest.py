# -*- coding: utf-8 -*-
import pytest

from ydb.tests.functional.security.lib.cluster_config import generate_certificates

pytest_plugins = [
    'ydb.tests.library.fixtures',
    'ydb.tests.library.flavours',
    'ydb.tests.functional.security.lib.mon_endpoints_fixtures',
]


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))

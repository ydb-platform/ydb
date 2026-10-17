# File-backed PDisks: node.stop/start keeps data, so the suite shares one cluster.
from ydb.tests.functional.nbs.lib.fixtures.cluster import nbs_cluster_file_pdisks as nbs_cluster  # noqa: F401

pytest_plugins = ['ydb.tests.functional.nbs.lib.fixtures.pytest_timeout_conf']

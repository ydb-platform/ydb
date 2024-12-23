import yatest.common
import os
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


def test_create_minidump():
    dump_path = os.path.join(yatest.common.tempfile.gettempdir(), 'dumps')
    os.makedirs(dump_path, exist_ok=True)
    os.environ['BREAKPAD_MINIDUMPS_PATH'] = dump_path
    cluster = KiKiMR(configurator=KikimrConfigGenerator(
        domain_name='local',
        extra_feature_flags=["enable_resource_pools"],
        use_in_memory_pdisks=True,
    ))
    cluster.start()
    for node in cluster.nodes.values():
        node.send_signal(6)
    try:
        cluster.stop()
    except BaseException:
        pass
    assert len(os.listdir(dump_path)) == len(cluster.nodes)

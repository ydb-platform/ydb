import yatest.common
import os
from ydb.tests.library.harness.kikimr_runner import KiKiMR


def test_create_minidump():
    dump_path = os.path.join(yatest.common.tempfile.gettempdir(), 'dumps')
    os.makedirs(dump_path, exist_ok=True)
    os.environ['BREAKPAD_MINIDUMPS_PATH'] = dump_path
    cluster = KiKiMR()
    cluster.start()
    for node in cluster.nodes.values():
        node.send_signal(6)
    try:
        cluster.stop()
    except RuntimeError:
        pass
    assert len(os.listdir(dump_path)) == len(cluster.nodes)

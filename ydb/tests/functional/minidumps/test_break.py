import yatest.common
import os
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


def test_create_minidump():
    dump_path = os.path.join(yatest.common.tempfile.gettempdir(), 'dumps1')
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


def __test_mindump_script(cluster: KiKiMR, dump_path, script_path):
    os.makedirs(dump_path, exist_ok=True)
    with open(script_path, 'w') as f:
        f.write(
            '#!/usr/bin/bash\n'
            'SUCCESS=$1\n'
            'PATH=$2\n'
            'echo $SUCCESS >${PATH}.success\n'
        )
    os.chmod(script_path, 0o777)
    cluster.start()
    for node in cluster.nodes.values():
        node.send_signal(6)
    try:
        cluster.stop()
    except RuntimeError:
        pass
    files = os.listdir(dump_path)
    dmps = list(filter(lambda x: x.endswith('.dmp'), files))
    successes = list(filter(lambda x: x.endswith('.dmp.success'), files))
    assert len(dmps) == len(cluster.nodes)
    assert len(successes) == len(cluster.nodes)


def test_minidump_script():
    temp_dir = yatest.common.tempfile.gettempdir()
    dump_path = os.path.join(temp_dir, 'dumps2')
    script_path = os.path.join(temp_dir, 'minidump_script.sh')
    os.environ['BREAKPAD_MINIDUMPS_SCRIPT'] = script_path
    os.environ['BREAKPAD_MINIDUMPS_PATH'] = dump_path
    __test_mindump_script(KiKiMR(), dump_path, script_path)


def test_minidump_script_args():
    temp_dir = yatest.common.tempfile.gettempdir()
    dump_path = os.path.join(temp_dir, 'dumps3')
    script_path = os.path.join(temp_dir, 'minidump_script.sh')
    os.environ['BREAKPAD_MINIDUMPS_SCRIPT'] = ''
    os.environ['BREAKPAD_MINIDUMPS_PATH'] = ''
    __test_mindump_script(KiKiMR(configurator=KikimrConfigGenerator(
        breakpad_minidumps_path=dump_path,
        breakpad_minidumps_script=script_path,
    )), dump_path, script_path)


def test_compatibility_info():
    res = yatest.common.subprocess.run(
        [yatest.common.binary_path(os.getenv('YDB_DRIVER_BINARY')), '--compatibility-info'],
        capture_output=True,
        encoding='utf8',
        check=True
    )
    assert res.stdout.find('HasInternalBreakpad: true') >= 0

import os
import subprocess
import sys

import yatest.common as yc


def test_run():
    lib_path = yc.build_path() + '/library/c/tvmauth/so/'
    include_path = yc.source_path() + '/library/c/tvmauth/'
    test_c_file = yc.source_path() + '/library/c/tvmauth/src/ut_export/main.c'
    test_cxx_file = yc.source_path() + '/library/c/tvmauth/src/ut_export/main.cpp'
    global_resources = yc.global_resources()
    sysroot = []
    os_sdk = (
        global_resources.get('SYSROOT_FOR_TEST_RESOURCE_GLOBAL')
        or global_resources.get('OS_SDK_ROOT_RESOURCE_GLOBAL')
        or global_resources.get('MACOS_SDK_RESOURCE_GLOBAL')
    )
    if os_sdk:
        sysroot = ['--sysroot', os_sdk]
    env = os.environ.copy()
    env.clear()
    env['LD_LIBRARY_PATH'] = lib_path + ':'
    if global_resources.get('LD_FOR_TEST_RESOURCE_GLOBAL'):
        env['PATH'] = global_resources.get('LD_FOR_TEST_RESOURCE_GLOBAL')
    if global_resources.get('COMPILER_FOR_TEST_RESOURCE_GLOBAL'):
        env['LD_LIBRARY_PATH'] += global_resources.get('COMPILER_FOR_TEST_RESOURCE_GLOBAL') + '/lib/:'
    if os_sdk:
        env['LD_LIBRARY_PATH'] += os_sdk + '/usr/lib/x86_64-linux-gnu/'
    subprocess.check_call(
        [yc.c_compiler_path(), '-I' + include_path, '-L' + lib_path, '-ltvmauth', '-std=c99', '-Werror', test_c_file] + sysroot,
        env=env,
    )
    subprocess.check_call(
        [
            './a.out',
        ],
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    subprocess.check_call(
        [yc.cxx_compiler_path(), '-I' + include_path, '-L' + lib_path, '-ltvmauth', '-std=c++11', '-Werror', test_cxx_file]
        + sysroot,
        env=env,
    )
    subprocess.check_call(
        [
            './a.out',
        ],
        env=env,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

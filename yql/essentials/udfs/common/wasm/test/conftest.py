import os
import shutil
import subprocess

import pytest

import yatest.common


def _repo_root():
    return yatest.common.source_path('.')


def _registry_dir():
    return os.path.join(
        _repo_root(),
        'yql/essentials/udfs/common/wasm/test/data/env_registry_complex',
    )


def _ensure_env_registry_complex():
    registry = _registry_dir()
    sdk_so = os.path.join(registry, 'sdk.so')
    module_so = os.path.join(registry, 'base64', 'libbase64-module.so')
    descriptor = os.path.join(registry, 'base64', 'function_descriptor.yson')

    if os.path.isfile(sdk_so) and os.path.isfile(module_so) and os.path.isfile(descriptor):
        return

    ya = os.path.join(_repo_root(), 'ya')
    if not os.path.isfile(ya):
        pytest.skip('ya not found; cannot build env_registry_complex artifacts')

    emscripten_platform = 'clang18-emscripten-wasm64'
    targets = [
        'yql/essentials/udfs/common/wasm/sdk',
        'yql/essentials/udfs/common/wasm/examples/base64/module',
    ]
    for target in targets:
        subprocess.check_call([
            ya,
            'make',
            f'--target-platform={emscripten_platform}',
            '--build=profile',
            target,
        ], cwd=_repo_root())

    os.makedirs(os.path.join(registry, 'base64'), exist_ok=True)
    shutil.copy2(
        os.path.join(
            _repo_root(),
            'yql/essentials/udfs/common/wasm/examples/base64/module/function_descriptor.yson',
        ),
        descriptor,
    )

    sdk_build = yatest.common.build_path('yql/essentials/udfs/common/wasm/sdk')
    module_build = yatest.common.build_path(
        'yql/essentials/udfs/common/wasm/examples/base64/module',
    )
    sdk_candidates = [
        os.path.join(sdk_build, 'libwasm-sdk.so'),
        os.path.join(sdk_build, 'sdk.so'),
    ]
    module_candidates = [
        os.path.join(module_build, 'libbase64-module.so'),
        os.path.join(module_build, 'libcommon-base64.so'),
    ]

    sdk_src = next((p for p in sdk_candidates if os.path.isfile(p)), None)
    module_src = next((p for p in module_candidates if os.path.isfile(p)), None)
    if sdk_src is None or module_src is None:
        pytest.skip('emscripten artifacts were not produced')

    shutil.copy2(sdk_src, sdk_so)
    shutil.copy2(module_src, module_so)


def pytest_collection_modifyitems(session, config, items):
    if any('EnvRegistryComplex' in item.name for item in items):
        _ensure_env_registry_complex()

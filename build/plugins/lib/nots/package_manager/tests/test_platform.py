import copy

import pytest

from build.plugins.lib.nots.package_manager.platform import package_supports_platform
from build.plugins.lib.nots.package_manager.lockfile import Lockfile


@pytest.mark.parametrize(
    "metadata,platform,supported,expected",
    [
        ({"os": ["darwin"], "cpu": ["arm64"]}, ("darwin", "arm64", "unknown"), None, True),
        ({"os": ["darwin"]}, ("linux", "x64", "glibc"), None, False),
        ({"os": ["!win32"]}, ("linux", "x64", "glibc"), None, True),
        ({"os": ["!win32"]}, ("win32", "x64", "unknown"), None, False),
        ({"os": ["any"]}, ("linux", "x64", "glibc"), None, True),
        ({"cpu": ["arm64"]}, ("linux", "x64", "glibc"), None, False),
        ({"libc": ["musl"]}, ("linux", "x64", "glibc"), None, False),
        ({"libc": ["musl"]}, ("linux", "x64", "unknown"), None, True),
        ({"os": ["darwin"]}, ("linux", "x64", "glibc"), {"os": ["current", "darwin"]}, True),
        ({"cpu": ["arm64"]}, ("linux", "x64", "glibc"), {"cpu": ["x64", "arm64"]}, True),
        ({"os": ["!linux"]}, ("linux", "x64", "glibc"), {"os": ["linux", "darwin"]}, False),
        ({"os": ["linux", "!linux"]}, ("linux", "x64", "glibc"), None, True),
        ({"os": ["!linux", "linux"]}, ("linux", "x64", "glibc"), None, False),
        ({"os": ["linux"]}, ("linux", "x64", "glibc"), {"os": []}, False),
        ({"os": ["darwin"]}, ("linux", "x64", "glibc"), ["darwin-arm64"], True),
        ({"os": "darwin"}, ("linux", "x64", "glibc"), None, True),
        ({"os": ["darwin"]}, None, None, True),
    ],
)
def test_platform_constraints(metadata, platform, supported, expected):
    assert package_supports_platform(metadata, platform, supported) is expected


def test_optional_contexts_and_lockfile_preservation():
    lock = Lockfile("/unused/pnpm-lock.yaml")
    resolution = {"integrity": "sha512-AA=="}
    lock.data = {
        "packages": {
            name + "@1.0.0": {"resolution": resolution.copy(), "os": ["win32"]}
            for name in ("optional", "required", "mixed", "unknown")
        },
        "snapshots": {
            "optional@1.0.0": {"optional": True},
            "required@1.0.0": {},
            "mixed@1.0.0(peer@1.0.0)": {"optional": True},
            "mixed@1.0.0(peer@2.0.0)": {},
        },
    }
    before = copy.deepcopy(lock.data)
    assert {p.key for p in lock.get_packages_meta(("darwin", "arm64", "unknown"))} == {
        "required@1.0.0",
        "mixed@1.0.0",
        "unknown@1.0.0",
    }
    assert len(list(lock.get_packages_meta())) == 4
    assert lock.data == before

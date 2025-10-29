import json
import os

import pytest
import library.python.resource as resource

from build.plugins.lib.nots.typescript.ts_glob import ts_glob, TsGlobConfig

test_tsconfig_real_files_prefix = "resfs/file/test-data/tsconfig-real-files/"
test_tsconfig_real_files_cases = [
    r[len(test_tsconfig_real_files_prefix) :] for r in resource.iterkeys("resfs/file/test-data/tsconfig-real-files/")
]


# see README.md for instruction to add more real-live cases
@pytest.mark.parametrize("test_case", test_tsconfig_real_files_cases)
def test_tsconfig_real_files(test_case):
    tsconfig = json.loads(resource.find(test_tsconfig_real_files_prefix + test_case).decode("utf-8"))
    compiler_options = tsconfig.get("compilerOptions", {})

    ts_glob_config = TsGlobConfig(
        root_dir=compiler_options.get("rootDir", "./"),
        out_dir=compiler_options.get("outDir", None),
        include=tsconfig.get("include", []),
    )

    all_files = [os.path.normpath(f) for f in tsconfig.get("files", [])]
    filtered_files = ts_glob(ts_glob_config, all_files)

    assert set(all_files) == set(filtered_files)


class TestTsGlobIncluding:
    ts_glob_config = TsGlobConfig(
        root_dir="src", out_dir="build", include=["src/module_a/**/*", "src/module_b/**/*.ts", "src/module_x"]
    )

    def test_dir_include(self):
        # arrange
        all_files = [
            "src/module_x/index.ts",
        ]

        # act + arrange
        assert ts_glob(self.ts_glob_config, all_files) == [
            "src/module_x/index.ts",
        ]

    def test_deep_include(self):
        # arrange
        all_files = [
            "src/module_a/index.ts",
            "src/module_b/index.ts",
            "src/module_c/index.ts",
        ]

        # act + arrange
        assert ts_glob(self.ts_glob_config, all_files) == [
            "src/module_a/index.ts",
            "src/module_b/index.ts",
        ]


class TestTsGlobExcluding:
    ts_glob_config = TsGlobConfig(root_dir="src", out_dir="build", include=["src/**/*"])

    def test_only_in_root_dir(self):
        # arrange
        all_files = [
            "CHANGELOG.md",
            "fake-src/one-more-src.ts",
            "src/index.ts",
        ]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == ["src/index.ts"]

    def test_exclude_out_dir(self):
        # arrange
        all_files = ["build/index.js"]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == []

    def test_exclude_out_dir_none(self):
        # arrange
        all_files = ["build/index.js"]
        ts_glob_config = TsGlobConfig(root_dir=".", out_dir=None)

        # act + assert
        assert ts_glob(ts_glob_config, all_files) == ["build/index.js"]

    def test_complex(self):
        # arrange
        all_files = [
            "CHANGELOG.md",
            "fake-src/one-more-src.ts",
            "src/baz.ts",
            "src/index.ts",
            "src/required_file.ts",
        ]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == ["src/baz.ts", "src/index.ts", "src/required_file.ts"]


class TestTsGlobNeedNormalization:
    ts_glob_config = TsGlobConfig(root_dir="./src", out_dir="./build", include=["./src/**/*"])

    def test_only_in_root_dir(self):
        # arrange
        all_files = [
            "CHANGELOG.md",
            "fake-src/one-more-src.ts",
            "src/index.ts",
        ]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == ["src/index.ts"]

    def test_exclude_out_dir(self):
        # arrange
        all_files = ["build/index.js"]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == []

    def test_exclude_out_dir_none(self):
        # arrange
        all_files = ["build/index.js"]
        ts_glob_config = TsGlobConfig(root_dir="./.", out_dir=None)

        # act + assert
        assert ts_glob(ts_glob_config, all_files) == ["build/index.js"]

    def test_complex(self):
        # arrange
        all_files = [
            "CHANGELOG.md",
            "fake-src/one-more-src.ts",
            "src/baz.ts",
            "src/index.ts",
            "src/required_file.ts",
        ]

        # act + assert
        assert ts_glob(self.ts_glob_config, all_files) == ["src/baz.ts", "src/index.ts", "src/required_file.ts"]

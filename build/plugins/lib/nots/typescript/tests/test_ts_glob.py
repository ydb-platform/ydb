from build.plugins.lib.nots.typescript.ts_glob import ts_glob, TsGlobConfig


class TestTsGlobIncluding:
    ts_glob_config = TsGlobConfig(
        root_dir="src", out_dir="build", include=["src/module_a/**/*", "src/module_b/**/*", "src/module_x"]
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

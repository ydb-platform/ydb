import pytest

from build.plugins.lib.nots.typescript import TsConfig, TsValidationError


def test_ts_config_validate_valid():
    cfg = TsConfig(path="/tsconfig.json")
    cfg.data = {
        "compilerOptions": {
            "rootDir": "./src",
            "outDir": "./build",
        },
    }

    cfg.validate()


def test_ts_config_validate_empty():
    cfg = TsConfig(path="/tsconfig.json")

    with pytest.raises(TsValidationError) as e:
        cfg.validate()

    assert e.value.errors == [
        "'rootDir' option is required",
        "'outDir' option is required",
    ]


def test_ts_config_validate_invalid_common():
    cfg = TsConfig(path="/tsconfig.json")
    cfg.data = {
        "compilerOptions": {
            "preserveSymlinks": True,
            "rootDirs": [],
            "outFile": "./foo.js",
        },
        "references": [],
        "files": [],
        "include": [],
        "exclude": [],
    }

    with pytest.raises(TsValidationError) as e:
        cfg.validate()

    assert e.value.errors == [
        "'rootDir' option is required",
        "'outDir' option is required",
        "'outFile' option is not supported",
        "'preserveSymlinks' option is not supported due to pnpm limitations",
        "'rootDirs' option is not supported, relative imports should have single root",
        "'files' option is not supported, use 'include'",
        "composite builds are not supported, use peerdirs in ya.make instead of 'references' option",
    ]


def test_ts_config_validate_invalid_subdirs():
    cfg = TsConfig(path="/foo/tsconfig.json")
    cfg.data = {
        "compilerOptions": {
            "rootDir": "/bar/src",
            "outDir": "../bar/build",
        },
    }

    with pytest.raises(TsValidationError) as e:
        cfg.validate()

    assert e.value.errors == [
        "'outDir' should be a subdirectory of the module",
    ]


def test_ts_config_compiler_options():
    cfg = TsConfig(path="/tsconfig.json")

    assert cfg.compiler_option("invalid") is None

    cfg.data = {
        "compilerOptions": {
            "rootDir": "src",
        },
    }

    assert cfg.compiler_option("rootDir") == "src"


class TestTsConfigMerge:
    def test_merge_paths(self):
        # arrange
        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {"compilerOptions": {"paths": {"path1": ["src/path1"], "path2": ["src/path2"]}}}

        cfg_common = TsConfig(path="/foo/tsconfig.common.json")
        cfg_common.data = {
            "compilerOptions": {"paths": {"path0": ["src/path0"]}},
        }

        # act
        cfg_main.merge(".", cfg_common)

        # assert
        assert cfg_main.data == {
            "compilerOptions": {"paths": {"path1": ["src/path1"], "path2": ["src/path2"]}},
        }

    def test_create_compiler_options(self):
        # arrange
        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {}

        cfg_common = TsConfig(path="/foo/config/tsconfig.common.json")
        cfg_common.data = {
            "compilerOptions": {
                "moduleResolution": "node",
            },
        }

        # act
        cfg_main.merge("config", cfg_common)

        # assert
        assert cfg_main.data == {
            "compilerOptions": {
                "moduleResolution": "node",
            },
        }

    def test_merge_compiler_options(self):
        # arrange
        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {
            "compilerOptions": {
                "esModuleInterop": True,
                "moduleResolution": "nodenext",
                "rootDir": "./src",
            },
            "extraField1": False,
            "sameField": False,
        }

        cfg_common = TsConfig(path="/foo/config/tsconfig.common.json")
        cfg_common.data = {
            "compilerOptions": {
                "moduleResolution": "node",
                "outDir": "./out",
                "strict": True,
            },
            "extraField2": True,
            "sameField": True,
        }

        # act
        cfg_main.merge("config", cfg_common)

        # assert
        assert cfg_main.data == {
            "compilerOptions": {
                "esModuleInterop": True,  # own value
                "moduleResolution": "nodenext",  # replaced value
                "outDir": "config/out",  # resolved path
                "rootDir": "./src",  # own path value (untouched)
                "strict": True,  # inherited value
            },
            "extraField1": False,  # own root field
            "extraField2": True,  # inherited root field
            "sameField": False,  # prefer own value
        }

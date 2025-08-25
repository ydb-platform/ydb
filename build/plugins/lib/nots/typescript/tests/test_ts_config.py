from pathlib import Path

import pytest

from build.plugins.lib.nots.typescript import TsConfig, TsValidationError


def test_ts_config_validate_valid(monkeypatch):
    monkeypatch.setattr(Path, "exists", lambda _: True)

    cfg = TsConfig(path="/target/tsconfig.json", source_dir="/target")
    cfg.data = {
        "compilerOptions": {
            "rootDir": "./src",
            "outDir": "./build",
        },
    }

    # Throws when outdir is not in use but defined in tsconfig
    with pytest.raises(TsValidationError) as e:
        cfg.validate(use_outdir=False)

    assert e.value.errors == [
        "'outDir' should be removed - it is not in use",
    ]

    # Passes well when outDir should be in use and is defined
    cfg.validate(use_outdir=True)


def test_ts_config_validate_empty():
    cfg = TsConfig(path="/tsconfig.json")

    # When outDir should not be used we got only one error
    with pytest.raises(TsValidationError) as e:
        cfg.validate(use_outdir=False)

    assert e.value.errors == [
        "'rootDir' option is required",
    ]

    # When outDir should be used we got two errors
    with pytest.raises(TsValidationError) as e:
        cfg.validate(use_outdir=True)

    assert e.value.errors == [
        "'rootDir' option is required",
        "'outDir' option is required",
    ]


def test_ts_config_declaration_with_dir():
    cfg = TsConfig(path="/tsconfig.json")
    cfg.data = {
        "compilerOptions": {"rootDir": "./src", "declaration": True, "declarationDir": "some/dir"},
    }

    cfg.validate(use_outdir=False)


def test_ts_config_declaration_without_dir():
    cfg = TsConfig(path="/tsconfig.json")
    cfg.data = {
        "compilerOptions": {"rootDir": "./src", "declaration": True},
    }

    # When outDir should not be used we got the error
    with pytest.raises(TsValidationError) as e:
        cfg.validate(use_outdir=False)

    assert e.value.errors == [
        "'declarationDir' option is required when 'declaration' is set",
    ]


def test_ts_config_declaration_with_outdir():
    cfg = TsConfig(path="/target/tsconfig.json", source_dir="/target")
    cfg.data = {
        "compilerOptions": {"rootDir": "./src", "outDir": "some/dir", "declaration": True},
    }

    # When we allow outDir it will be enought to set it
    cfg.validate(use_outdir=True)


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
        cfg.validate(use_outdir=True)

    assert e.value.errors == [
        "'rootDir' option is required",
        "'outDir' option is required",
        "'outFile' option is not supported",
        "'preserveSymlinks' option is not supported due to pnpm limitations",
        "composite builds are not supported, use peerdirs in ya.make instead of 'references' option",
    ]


def test_ts_config_validate_invalid_local_outdir():
    cfg = TsConfig(path="/tsconfig.json")
    for out_dir in [".", "", "./"]:
        cfg.data = {
            "compilerOptions": {
                "rootDir": "./",
                "outDir": out_dir,
            },
        }

        with pytest.raises(TsValidationError) as e:
            cfg.validate(use_outdir=True)

        assert e.value.errors == [
            "'outDir' value '{}' is not supported, use directory name like 'build'".format(out_dir),
        ]


def test_ts_config_validate_invalid_subdirs():
    cfg = TsConfig(path="/foo/tsconfig.json", source_dir="/foo")
    cfg.data = {
        "compilerOptions": {
            "rootDir": "/bar/src",
            "outDir": "../bar/build",
        },
    }

    with pytest.raises(TsValidationError) as e:
        cfg.validate(use_outdir=True)

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


class TestTsConfigGetConfigDir:
    def test_simple(self):
        # arrange
        cfg = TsConfig(path="/target/config/tsconfig.json", source_dir="/target")

        # act
        result = cfg.get_config_dir()

        # assert
        assert result == Path("/target/config")


class TestTsConfigGetModuleDir:
    def test_with_moddir(self):
        # arrange
        cfg = TsConfig(path="/target/config/tsconfig.json", source_dir="/target")

        # act
        result = cfg.get_module_dir()

        # assert
        assert result == Path("/target")

    def test_without_moddir(self, monkeypatch):
        # arrange
        monkeypatch.setattr(Path, "exists", lambda _: True)
        cfg = TsConfig(path="/target/config/tsconfig.json")

        # act
        result = cfg.get_module_dir()

        # assert
        assert result == Path("/target")


class TestTsConfigGetOutputDir:
    def test_simple(self):
        # arrange
        cfg = TsConfig(path="/target/tsconfig.json", source_dir="/target")
        cfg.data = {
            "compilerOptions": {"rootDir": "./src", "outDir": "./build"},
        }

        # act
        result = cfg.get_out_dirs()

        # assert
        assert result == {"build"}

    def test_with_declaration_dir(self):
        # arrange
        cfg = TsConfig(path="/target/tsconfig.json", source_dir="/target")
        cfg.data = {
            "compilerOptions": {"rootDir": "./src", "outDir": "./build", "declarationDir": "./types"},
        }

        # act
        result = cfg.get_out_dirs()

        # assert
        assert result == {"build", "types"}

    def test_from_dir(self):
        # arrange
        cfg = TsConfig(path="/target/configs/tsconfig.json", source_dir="/target")
        cfg.data = {
            "compilerOptions": {"rootDir": "../src", "outDir": "../build"},
        }

        # act
        result = cfg.get_out_dirs()

        # assert
        assert result == {"build"}


class TestTsConfigRelPath:
    def test_simple(self):
        # arrange
        cfg = TsConfig(path="/target/tsconfig.json", source_dir="/target")

        # act
        result = cfg.rel_to_module_path("./src")

        # assert
        assert result == Path("src")

    def test_from_dir(self):
        # arrange
        cfg = TsConfig(path="/target/configs/tsconfig.json", source_dir="/target")

        # act
        result = cfg.rel_to_module_path("../src")

        # assert
        assert str(result) == "src"

    def test_raise_exception(self, monkeypatch):
        # arrange
        cfg = TsConfig(path="/target/tsconfig.json")
        monkeypatch.setattr(cfg, 'get_module_dir', lambda: None)

        # act+assert
        with pytest.raises(Exception, match="Can't find the module directory"):
            cfg.rel_to_module_path("./src")


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

    def test_fix_path_aliases(self):
        # arrange
        cfg_main = TsConfig(path="/foo/tsconfig.json")

        cfg_common = TsConfig(path="/foo/configs/tsconfig.common.json")
        cfg_common.data = {
            "compilerOptions": {"paths": {"path1": ["../src/path1"], "path2": ["../src/path2"]}},
        }

        # act
        cfg_main.merge("config", cfg_common)

        # assert
        assert cfg_main.data['compilerOptions']['paths']['path1'] == ["./src/path1"]
        assert cfg_main.data == {
            "compilerOptions": {"paths": {"path1": ["./src/path1"], "path2": ["./src/path2"]}},
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


class TestTsConfigExtends:
    def create_empty_ts_config(self, path, source_dir: str = None):
        cfg = TsConfig(path=path, source_dir=source_dir)
        cfg.data = {}
        return cfg

    def create_ts_config_with_data_once(self, path, source_dir: str = None):
        cfg = TsConfig(path=path, source_dir=source_dir)

        if path == "/foo/base-tsconfig.json":
            cfg.data = {"extends": "./extends/recursive/tsconfig.json"}
        else:
            cfg.data = {}

        return cfg

    def test_extends_empty(self):
        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {}

        paths = cfg_main.inline_extend({})

        assert paths == []

    def test_extends_single_with_dot(self, monkeypatch):
        monkeypatch.setattr(TsConfig, "load", self.create_empty_ts_config)

        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = dict({"extends": "./extends/tsconfig.json"})

        paths = cfg_main.inline_extend({})

        assert paths == ["/foo/extends/tsconfig.json"]

    def test_extends_single_without_dot(self, monkeypatch):
        monkeypatch.setattr(TsConfig, "load", self.create_empty_ts_config)

        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = dict({"extends": "extends/tsconfig.json"})

        paths = cfg_main.inline_extend({"extends": "dir/extends"})

        assert paths == ["/foo/dir/extends/tsconfig.json"]

    def test_extends_array(self, monkeypatch):
        monkeypatch.setattr(TsConfig, "load", self.create_empty_ts_config)

        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {"extends": ["extends/tsconfig1.json", "extends/tsconfig2.json"]}

        paths = cfg_main.inline_extend({"extends": "dir/extends"})

        assert paths == ["/foo/dir/extends/tsconfig1.json", "/foo/dir/extends/tsconfig2.json"]

    def test_extends_empty_array(self):
        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {"extends": []}

        paths = cfg_main.inline_extend({})

        assert paths == []

    def test_recursive_extend(self, monkeypatch):
        monkeypatch.setattr(TsConfig, "load", self.create_ts_config_with_data_once)

        cfg_main = TsConfig(path="/foo/tsconfig.json")
        cfg_main.data = {"extends": "./base-tsconfig.json"}

        paths = cfg_main.inline_extend({})

        assert paths == ["/foo/base-tsconfig.json", "/foo/extends/recursive/tsconfig.json"]

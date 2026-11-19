import base64
import json
import sys
from types import SimpleNamespace

import nots
import pytest

from nots import _library_for_paths, _parse_ts_checks, _ts_check_size


@pytest.mark.parametrize("hermetic", [None, "no", "yes"])
@pytest.mark.parametrize("local_cli", ["no", "yes"])
@pytest.mark.parametrize("supported", ["no", "yes"])
def test_hermetic_node_modules_requires_explicit_enablement(hermetic, local_cli, supported):
    values = {
        "TS_HERMETIC_NODE_MODULES": hermetic,
        "TS_LOCAL_CLI": local_cli,
        "_SUPPORTS_HERMETIC_NODE_MODULES": supported,
    }
    raw_values = dict(values, TS_LOCAL_CLI="$_TS_LOCAL_CLI_DEFAULT")
    unit = SimpleNamespace(get=raw_values.get, get_subst=values.get)

    assert nots._use_hermetic_node_modules(unit) == (
        hermetic == "yes" and local_cli == "no" and supported == "yes"
    )


@pytest.mark.parametrize(
    ("for_mod", "expected_error"),
    [
        ("devtools/dummy_arcadia/typescript/simple_jest", None),
        (".", "Tests should be for parent module"),
        ("./", "Tests should be for parent module"),
        ("../simple_jest", "You are using a relative path for a module"),
    ],
)
def test_ts_test_for_validates_target_path(monkeypatch, for_mod, expected_error):
    errors = []
    unit = SimpleNamespace(get=lambda name: "no" if name == "_ALLOW_REL_FOR_PATH" else "")
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)

    nots._VALIDATE_TS_TEST_FOR_ARGS(unit, for_mod, "${ARCADIA_ROOT}")

    if expected_error:
        assert len(errors) == 1
        assert expected_error in errors[0]
    else:
        assert errors == []


def test_parse_ts_checks_preserves_command_with_spaces_and_multiple_checks():
    raw = (
        "$_TS_CHECK_LIST~~~nots:lint lint no "
        "~~~nots:custom lint yes eslint src --max-warnings 0 "
        "~~~nots:test test no"
    )

    assert _parse_ts_checks(raw, "~~~") == [
        ["nots:lint", "lint", "no", ""],
        ["nots:custom", "lint", "yes", "eslint src --max-warnings 0"],
        ["nots:test", "test", "no", ""],
    ]


@pytest.mark.parametrize(
    ("check_type", "timeout_medium", "expected_size"),
    [
        ("lint", "no", "SMALL"),
        ("lint", "yes", "MEDIUM"),
        ("test", "no", None),
        ("test", "yes", "MEDIUM"),
    ],
)
def test_ts_check_size(check_type, timeout_medium, expected_size):
    assert _ts_check_size(check_type, timeout_medium) == expected_size


@pytest.mark.parametrize(
    ("target", "source", "build"),
    [
        ("apps/app/storybook", "apps/app", "apps/app__storybook.5cbbd79a82b1b2f1230b09c5ffc3399c"),
        ("apps/app/./storybook", "apps/app", "apps/app__storybook.5cbbd79a82b1b2f1230b09c5ffc3399c"),
        (
            "apps/app/examples/storybook",
            "apps/app",
            "apps/app__examples__storybook.deec0c58db6b8098c3cca86d0222c41f",
        ),
        (
            "apps/app/examples__storybook",
            "apps/app",
            "apps/app__examples__storybook.9be05a7729604fbea954bfac1d86f3d3",
        ),
    ],
)
def test_library_for_paths(target, source, build):
    assert _library_for_paths(target, source) == (source, build)


@pytest.mark.parametrize("source", ["apps/app/storybook", "apps/other", "../app", "/apps/app", "."])
def test_library_for_rejects_non_parent_sources(source):
    with pytest.raises(ValueError, match="parent project"):
        _library_for_paths("apps/app/storybook", source)


@pytest.mark.parametrize(
    "settings",
    [
        {"_WITH_NODE_MODULES": "yes"},
        {"_WITH_NODE_MODULES": "yes", "_WITH_NODE_MODULES_PROD": "yes"},
        {"NODE_MODULES_FOR": "apps/other:apps/app/storybook"},
    ],
)
def test_library_for_rejects_node_modules_bundle(monkeypatch, settings):
    errors = []
    values = {"_TS_LIBRARY_FOR": "yes", "MODDIR": "apps/app/storybook", **settings}
    unit = SimpleNamespace(get=values.get)
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)

    nots._TS_LIBRARY_CONFIGURE(unit)

    assert errors == ["TS_LIBRARY_FOR does not support WITH_NODE_MODULES or NODE_MODULES_FOR"]


@pytest.mark.parametrize(
    ("library_for", "build_command", "expected_commands"),
    [
        ("no", "vite build --legacy", []),
        ("yes", "pnpm run build:storybook", ["pnpm run build:storybook"]),
        ("yes", "vite build --config custom.ts", ["vite build --config custom.ts"]),
    ],
)
def test_library_configure_preserves_regular_build_script_behavior(
    monkeypatch, library_for, build_command, expected_commands
):
    values = {
        "_TS_LIBRARY_FOR": library_for,
        "_TS_BUILD_SCRIPT": "build",
        "_TS_OUTPUTS": "dist",
        "_TS_BUILD_COMMAND": build_command,
    }
    unit = SimpleNamespace(get=values.get, set=lambda pair: values.update([pair]))
    package_json = SimpleNamespace(get_files=lambda: ["dist"])
    package_manager = SimpleNamespace(sources_path="apps/app", load_package_json_from_dir=lambda _: package_json)
    monkeypatch.setattr(nots, "_create_pm", lambda _: package_manager)
    commands = []
    monkeypatch.setattr(nots, "_set_ts_build_command", lambda _, command: commands.append(command))

    nots._TS_LIBRARY_CONFIGURE(unit)

    assert commands == expected_commands


@pytest.mark.parametrize("hermetic", [False, True])
@pytest.mark.parametrize("check_type", ["lint", "test"])
def test_ts_check_preserves_data_and_runs_user_recipes_after_setup(monkeypatch, hermetic, check_type):
    image = "docker://registry.yandex.net/test@sha256:" + "a" * 64 + "=test:latest"
    policy = "arcadia/devtools/frontend_build_platform/nots/tier0-settings/src/settings.yaml"
    values = {
        "_TS_CHECK_LIST": f"~~~nots:check {check_type} no",
        "TEST_DATA_VALUE": "arcadia/app/fixture.json",
        "_TS_CHECK_SEPARATOR": "~~~",
        "TEST_RECIPES_VALUE": "podman-compose",
        "DOCKER_IMAGES_VALUE": image,
    }

    def append_recipe(name):
        values["TEST_RECIPES_VALUE"] += " " + name
        if name == "install-node-modules":
            values["TEST_DATA_VALUE"] += " " + policy

    records = []
    unit = SimpleNamespace(
        get=lambda name: values.get(name, ""),
        get_subst=lambda name: values.get(name, ""),
        set=lambda pair: values.update([pair]),
        enabled=lambda _: False,
        set_property=lambda pair: records.append(pair),
        on_setup_extract_node_modules_layer_recipe=lambda _: append_recipe("extract-node-modules-layer"),
        on_setup_install_node_modules_recipe=lambda _: append_recipe("install-node-modules"),
        on_setup_extract_output_tars_recipe=lambda _: append_recipe("extract-outputs"),
    )
    pm = SimpleNamespace(
        module_path="app",
        sources_path="app",
        get_local_peers_from_package_json=lambda: [],
        load_package_json_from_dir=lambda _: SimpleNamespace(data={}),
    )
    monkeypatch.setattr(nots, "_create_pm", lambda _: pm)
    monkeypatch.setattr(nots, "_use_hermetic_node_modules", lambda _: hermetic)
    monkeypatch.setattr(nots, "_create_erm_json", lambda _: "{}")
    monkeypatch.setattr(nots.df.TestFiles, "ts_check_srcs", lambda *_: "src/test.ts")

    create_dart_record = nots.create_dart_record

    def recipe_data_and_image_record(fields, unit, flat_args, spec_args):
        # Keep real field selection and serialization, without unrelated toolchain metadata.
        return create_dart_record(
            [field for field in fields if field.__self__ in (nots.df.TestRecipes, nots.df.TestData, nots.df.DockerImage)],
            unit,
            flat_args,
            spec_args,
        )

    monkeypatch.setattr(nots, "create_dart_record", recipe_data_and_image_record)
    monkeypatch.setattr(nots.ytest, "dump_test", lambda _, record: record)

    nots._TS_CHECK_CONFIGURE(unit, "TS_TEST_FOR")

    assert len(records) == 1
    record = records[0][1]
    if check_type == "test":
        assert nots.df.deserialize_list(record[nots.df.DockerImage.KEY]) == [image]
    expected_data = ["arcadia/app/fixture.json"] + ([] if hermetic else [policy])
    assert nots.df.deserialize_list(record[nots.df.TestData.KEY]) == expected_data
    setup = "extract-node-modules-layer" if hermetic else "install-node-modules"
    assert base64.b64decode(record[nots.df.TestRecipes.KEY]).decode().split() == [
        setup,
        "extract-outputs",
        "podman-compose",
    ]


@pytest.mark.parametrize(
    "settings,expected",
    [
        ({"HOST_OS_DARWIN": "yes", "HOST_ARCH_ARM64": "yes"}, ("darwin", "arm64", "unknown")),
        ({"HOST_OS_LINUX": "yes", "HOST_ARCH_X86_64": "yes"}, ("linux", "x64", "unknown")),
        ({"HOST_OS_LINUX": "yes", "HOST_ARCH_ARM64": "yes"}, ("linux", "arm64", "unknown")),
        ({"HOST_OS_WINDOWS": "yes", "HOST_ARCH_X86_64": "yes"}, ("win32", "x64", "unknown")),
        ({"HOST_OS_LINUX": "yes"}, None),
        (
            {"OS_LINUX": "yes", "ARCH_X86_64": "yes", "HOST_OS_DARWIN": "yes", "HOST_ARCH_ARM64": "yes"},
            ("darwin", "arm64", "unknown"),
        ),
    ],
)
def test_tarballs_platform_uses_action_configuration(settings, expected):
    values = dict(settings)
    unit = SimpleNamespace(get=lambda name: values.get(name, ""), set=lambda pair: values.update([pair]))
    assert nots._configure_tarballs_platform(unit) == expected
    if expected:
        assert values["_PREPARE_DEPS_PLATFORM_ARG"] == "--tarballs-platform " + " ".join(expected)
    else:
        assert "_PREPARE_DEPS_PLATFORM_ARG" not in values


@pytest.mark.parametrize("configure", [nots._TS_PROTO_CONFIGURE, nots._PREPARE_DEPS_CONFIGURE])
def test_non_recursive_proto_requires_auto_package(monkeypatch, configure):
    errors = []
    unit = SimpleNamespace(
        get=lambda key: "yes" if key in ("_TS_PROTO_NON_RECURSIVE", "TS_PROTO_PREPARE_DEPS") else "",
        message=lambda value: None,
    )
    monkeypatch.setattr(nots, "_is_ts_proto_auto", lambda unit: False)
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)
    configure(unit)
    assert len(errors) == 1
    assert "TS_PROTO_NON_RECURSIVE() requires auto-proto" in errors[0]


def test_non_recursive_auto_proto_attaches_only_auto_peers(monkeypatch):
    peers = []
    unit = SimpleNamespace(
        get=lambda key: "yes" if key in ("TS_PROTO_PREPARE_DEPS", "_TS_PROTO_NON_RECURSIVE") else "",
        get_subst=lambda key: "proto/auto proto/custom proto/raw",
        resolve=lambda path: path,
        onpeerdir=peers.extend,
        on_ts_proto_auto_prepare_deps_configure=lambda: None,
    )
    monkeypatch.setattr(nots, "_is_ts_proto_auto", lambda unit: True)
    monkeypatch.setattr(nots, "_is_real_file", lambda path: path == "$S/proto/custom/package.json")
    nots._PREPARE_DEPS_CONFIGURE(unit)
    assert peers == ["proto/auto", "proto/raw"]


@pytest.mark.parametrize("runtime_flag", ["_WITH_NODEJS_BIN", "_WITH_BUN_BIN"])
@pytest.mark.parametrize("entry_type", ["file", "directory"])
@pytest.mark.parametrize("library_for", [False, True])
def test_runtime_rejects_source_namespace_collision(monkeypatch, tmp_path, runtime_flag, entry_type, library_for):
    source = tmp_path / "source"
    source.mkdir()
    entry = source / ".nots-runtime"
    if entry_type == "file":
        entry.write_text("user data")
    else:
        entry.mkdir()

    errors = []
    commands = []
    values = {runtime_flag: "yes"}
    if library_for:
        values.update(TS_FOR="yes", TS_FOR_DIR=str(source))
    unit = SimpleNamespace(
        get=values.get,
        path=lambda: str(tmp_path / "module") if library_for else str(source),
        resolve=lambda path: path,
        set=commands.append,
    )
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)
    nots._TS_RUNTIME_BIN_CONFIGURE(unit)
    assert len(errors) == 1
    assert ".nots-runtime is reserved" in errors[0]
    assert commands == []


def test_runtime_allows_output_symlink(monkeypatch, tmp_path):
    output = tmp_path / "build-output"
    output.mkdir()
    (tmp_path / ".nots-runtime").symlink_to(output, target_is_directory=True)
    commands = []
    unit = SimpleNamespace(
        get=lambda key: "yes" if key == "_WITH_NODEJS_BIN" else "",
        path=lambda: str(tmp_path),
        resolve=lambda path: path,
        set=commands.append,
    )
    errors = []
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)
    nots._TS_RUNTIME_BIN_CONFIGURE(unit)
    assert not errors
    assert commands


@pytest.mark.parametrize("library_for", [False, True])
@pytest.mark.parametrize("local_yaml", [False, True])
@pytest.mark.parametrize("common_yaml", [False, True])
def test_settings_files_invalidate_graph_configuration(monkeypatch, tmp_path, library_for, local_yaml, common_yaml):
    from build.plugins.lib.nots.package_manager import PackageManager, common_config, utils

    # ymake loads plugin libraries under lib.*, unlike the standalone Python test binary.
    monkeypatch.setitem(sys.modules, "lib.nots.package_manager.common_config", common_config)
    monkeypatch.setitem(sys.modules, "lib.nots.package_manager.utils", utils)
    source = "apps/app"
    project = tmp_path / source
    project.mkdir(parents=True)
    manifest = {}
    nots_settings = {"commonConfigPath": "../common.yaml"} if common_yaml else {}
    if common_yaml:
        (project.parent / "common.yaml").write_text("patchedDependencies: {common: patches/common.patch}\n")
    if local_yaml:
        (project / "nots.yaml").write_text(
            "nots: " + json.dumps(nots_settings) + "\npatchedDependencies: {local: patches/local.patch}\n"
        )
    elif common_yaml:
        manifest["nots"] = nots_settings
    (project / "package.json").write_text(json.dumps(manifest))
    pm = PackageManager("$B", "$B/" + source, str(project), module_path=source, sources_root=str(tmp_path))
    values = {"MODDIR": source + "/storybook" if library_for else source}
    errors = []
    unit = SimpleNamespace(
        get=lambda name: values.get(name, ""),
        get_subst=lambda name: values.get(name, ""),
        set=lambda pair: values.update([pair]),
        resolve=lambda value: value.replace("$S", str(tmp_path)),
        onpeerdir=lambda _: None,
        ondepends=lambda _: None,
    )
    monkeypatch.setattr(nots, "_create_pm", lambda _: pm)
    monkeypatch.setattr(nots, "_use_hermetic_node_modules", lambda _: False)
    monkeypatch.setattr(nots, "_SET_TS_FOR_VARS", lambda *_: None)
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)

    if library_for:
        nots._TS_LIBRARY_FOR_ARGS(unit, source)
    else:
        nots._prepare_deps_configure(unit)

    assert errors == []
    dependencies = values.get("_MAKEFILE_INCLUDE_LIKE_DEPS", "").split()
    configs = [
        dependency
        for dependency in dependencies
        if dependency.endswith(".yaml") and not dependency.endswith("/pnpm-lock.yaml")
    ]
    expected = []
    if local_yaml:
        expected.append("${ARCADIA_ROOT}/apps/app/nots.yaml")
    if common_yaml:
        expected.append("${ARCADIA_ROOT}/apps/common.yaml")
    assert sorted(configs) == sorted(expected)
    assert not any(dependency.endswith(".patch") for dependency in dependencies)

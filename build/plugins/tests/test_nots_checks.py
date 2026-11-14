import base64
from types import SimpleNamespace

import nots
import pytest

from nots import _library_for_paths, _parse_ts_checks, _ts_check_size


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
    policy = "arcadia/devtools/frontend_build_platform/nots/constants/src/tier0-settings.yaml"
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

import json

from build.plugins.lib.nots.package_manager import utils


def test_extract_package_name_from_path():
    happy_checklist = [
        ("@yandex-int/foo-bar-baz/some/path/inside/the/package", "@yandex-int/foo-bar-baz"),
        ("@yandex-int/foo-bar-buzz", "@yandex-int/foo-bar-buzz"),
        ("package-wo-scope", "package-wo-scope"),
        ("p", "p"),
        ("", ""),
    ]

    for item in happy_checklist:
        package_name = utils.extract_package_name_from_path(item[0])
        assert package_name == item[1]


def test_remove_node_modules_volatile_metadata_handles_long_json_key(tmp_path):
    node_modules_path = tmp_path / "node_modules"
    node_modules_path.mkdir()

    long_dependency_key = "dependency@" + "x" * 1100
    modules_yaml_path = node_modules_path / ".modules.yaml"
    modules_yaml = json.dumps(
        {
            "hoistedDependencies": {long_dependency_key: {"private": False}},
            "prunedAt": "2026-08-14T00:00:00.000Z",
            "storeDir": "/build/.nots/pnpm_store/v10",
            "virtualStoreDir": "node_modules/.pnpm",
        },
        indent=2,
    )
    modules_yaml_path.write_text(modules_yaml)

    utils.remove_node_modules_volatile_metadata(str(node_modules_path))

    expected_modules_yaml = modules_yaml.replace('  "prunedAt": "2026-08-14T00:00:00.000Z",\n', "").replace(
        '  "storeDir": "/build/.nots/pnpm_store/v10",\n', ""
    )
    assert modules_yaml_path.read_text() == expected_modules_yaml

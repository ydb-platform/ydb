import pytest
import json
import os


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "test_case: Test case (github issue) identifier"
    )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Receiving report object
    outcome = yield
    report = outcome.get_result()

    if call.when == "call":
        test_meta_dir = os.getenv("TEST_META_INFO")

        if test_meta_dir:
            # Check if test_case marker is set
            marker = item.get_closest_marker("test_case")
            if marker:
                filesystempath = report.location[0]
                domain_info = report.location[2]

                user_properties_file_name = filesystempath.replace("/", "_") + "_" + domain_info.replace(":", "_") + ".json"
                user_properties_file_name = os.path.join(test_meta_dir, user_properties_file_name)
                assert not os.path.isfile(user_properties_file_name)

                user_properties = {}

                if filesystempath not in user_properties:
                    user_properties[filesystempath] = {}

                if domain_info not in user_properties[filesystempath]:
                    user_properties[filesystempath][domain_info] = {}

                test_case_id = marker.args[0]
                if not test_case_id.startswith("#"):
                    assert False, "pytest.mark.test_case(id), id must start with #, ie pytest.mark.test_case(#100)"

                test_case_id = test_case_id.replace("#", "")

                props = [("url:test_case",
                          f'https://github.com/ydb-platform/ydb/issues/{test_case_id}')]

                for prop in props:
                    report.user_properties.append(prop)

                for prop in report.user_properties:
                    user_properties[filesystempath][domain_info][prop[0]] = prop[1]

                with open(user_properties_file_name, "w") as upf:
                    json.dump(user_properties, upf, indent=4)

    return outcome

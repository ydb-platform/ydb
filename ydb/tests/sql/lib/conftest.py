import pytest
import json
import os.path


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "test_case: Test case (github issue) identifier"
    )


@pytest.hookimpl(tryfirst=True,hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # Receiving report object
    outcome = yield
    report = outcome.get_result()

    if call.when == "teardown":
        # Check if test_case marker is set
        marker = item.get_closest_marker("test_case")
        if marker:
            user_properties_file_name = "pytest_user_properties.json"
            user_properties = {}
            if os.path.isfile(user_properties_file_name):
                with open(user_properties_file_name, "r") as upf:
                    user_properties = json.load(upf)

            filesystempath = report.location[0]

            if filesystempath not in user_properties:
                user_properties[filesystempath] = {}

            domain_info = report.location[2]
            if domain_info not in user_properties[filesystempath]:
                user_properties[filesystempath][domain_info] = {}

            test_case_id = marker.args[0]
            test_case_id = test_case_id.replace("#", "")

            props = [("url:test_case",
                      f'https://github.com/ydb-platform/ydb/issues/{test_case_id}')]

            for prop in props:
                report.user_properties.append(prop)

            for prop in report.user_properties:
                user_properties[filesystempath][domain_info][prop[0]] = prop[1]

            with open(user_properties_file_name, "w") as upf:
                json.dump(user_properties, upf)

    return outcome

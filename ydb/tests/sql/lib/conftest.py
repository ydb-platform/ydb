import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "test_case: Test case (github issue) identifier"
    )

@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    # Receiving report object
    report = yield

    if call.when == "call":
        # Check if test_case marker is set
        marker = item.get_closest_marker("test_case")
        if marker:
            test_case_id = marker.args[0]
            test_case_id = test_case_id.replace("#", "")

            report.user_properties.append(("url:test_case",
                                           f'https://github.com/ydb-platform/ydb/issues/{test_case_id}'))

    return report


import pytest

link_test_case = pytest.mark.test_case


def skip_with_issue(issue_id):
    return pytest.mark.skip(reason=issue_id)

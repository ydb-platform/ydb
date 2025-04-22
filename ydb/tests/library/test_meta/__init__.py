import pytest

link_test_case = pytest.mark.test_case
skip_with_issue = lambda issue_id: pytest.mark.skip(reason=issue_id)

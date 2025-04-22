import pytest

link_test_case = pytest.mark.test_case
skip_with_issue = lambda id_: pytest.mark.skip(reason=id_)

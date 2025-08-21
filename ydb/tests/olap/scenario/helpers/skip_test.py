import pytest
from ydb.tests.olap.scenario.helpers import TestContext
from ydb.tests.olap.lib.utils import get_external_param


def check_test_for_skipping(ctx: TestContext):
    current_test = f"{ctx.suite}::{ctx.test}"
    skipped_tests = get_external_param('olap-skip', '').split(',')
    if current_test != '' and current_test in skipped_tests:
        pytest.skip("marked as skipped in params")

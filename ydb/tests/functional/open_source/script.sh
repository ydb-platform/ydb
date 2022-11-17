#!/bin/sh

ya make -t --test-prepare
export PYTHONPATH=${PYTHONPATH}:${HOME}/arcadia/library/python/testing:${HOME}/arcadia/library/python/pytest/plugins:${HOME}/arcadia/library/python/testing/yatest_common:${HOME}/arcadia
export YA_TEST_CONTEXT_FILE=`readlink test-results/py3test/test.context`
PYTEST_PLUGINS=ya,conftests pytest test_yatest_common.py

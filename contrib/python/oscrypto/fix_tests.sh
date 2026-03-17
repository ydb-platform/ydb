#!/usr/bin/env bash

set -x
# note: sed -i '' 'expr' is macos/bsd specific, remove first '' for linux

# fix imports
find tests -type f -name "*.py" -print0 | xargs -0 sed -i '' 's/^tests_root = os.path.dirname(__file__)/#tests_root = os.path.dirname(__file__)\nfrom yatest.common import test_source_path\ntests_root = test_source_path()/g'

## disable test_classes in __init__.py
#sed -i '' 's/^def test_classes():/import pytest\n@pytest.mark.skip\ndef test_classes():/g' tests/__init__.py

#!/usr/bin/env python3
"""Temporary CI experiment: stable test UIDs without cached test results."""

import os
import sys

if not getattr(sys, "is_standalone_binary", False):
    os.environ["Y_PYTHON_ENTRY_POINT"] = ":main"
    os.execv("./ya", ["./ya", os.path.abspath(__file__)] + sys.argv[1:])

# Child tools must use their own entry points.
os.environ.pop("Y_PYTHON_ENTRY_POINT", None)

from devtools.ya.test import opts, test_node

original_postprocess = opts.UidCalculationOptions.postprocess


def postprocess(self):
    self.force_retest = True
    original_postprocess(self)


opts.UidCalculationOptions.postprocess = postprocess
test_node._need_random_uid = lambda suite, options: test_node.in_canonize_mode(options)
test_node._stable_dir_outputs = lambda suite, options: (
    test_node._should_cache_suite(suite, options)
    and not test_node.in_canonize_mode(options)
    and options.dir_outputs_test_mode
)

sys.argv = [sys.executable] + sys.argv[1:]
from entry.main import main

main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys

def check_file_contains(filepath, pattern, description):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            if re.search(pattern, content, re.MULTILINE):
                print(f"✓ {description}")
                return True
            else:
                print(f"✗ {description}")
                print(f"  Pattern not found: {pattern}")
                return False
    except Exception as e:
        print(f"✗ Failed to check {filepath}: {e}")
        return False


def main():
    print("=" * 60)
    print("Module Parameter Code Validation")
    print("=" * 60)
    print()
    
    results = []
    
    # Check 1: module parameter in KikimrConfigGenerator.__init__
    print("Checking kikimr_config.py...")
    results.append(check_file_contains(
        "ydb/tests/library/harness/kikimr_config.py",
        r"module=None",
        "module parameter added to __init__ signature"
    ))
    
    results.append(check_file_contains(
        "ydb/tests/library/harness/kikimr_config.py",
        r"self\.module\s*=\s*module",
        "module parameter stored in instance variable"
    ))
    
    # Check 2: --module handling in kikimr_runner.py
    print("\nChecking kikimr_runner.py...")
    results.append(check_file_contains(
        "ydb/tests/library/harness/kikimr_runner.py",
        r"if\s+self\.__configurator\.module\s+is\s+not\s+None:",
        "module parameter check in command generation"
    ))
    
    results.append(check_file_contains(
        "ydb/tests/library/harness/kikimr_runner.py",
        r'--module=%s.*self\.__configurator\.module',
        "--module argument added to command"
    ))
    
    # Check 3: Test file exists
    print("\nChecking test file...")
    results.append(check_file_contains(
        "ydb/tests/functional/config/test_module_parameter.py",
        r"class TestModuleParameter",
        "TestModuleParameter class created"
    ))
    
    results.append(check_file_contains(
        "ydb/tests/functional/config/test_module_parameter.py",
        r'module="TEST-MODULE-01"',
        "module parameter used in test"
    ))
    
    results.append(check_file_contains(
        "ydb/tests/functional/config/test_module_parameter.py",
        r"test_cluster_starts_and_is_operational_with_module_parameter",
        "combined test for cluster start and operation"
    ))
    
    # Check 4: Test added to ya.make
    print("\nChecking ya.make...")
    results.append(check_file_contains(
        "ydb/tests/functional/config/ya.make",
        r"test_module_parameter\.py",
        "test_module_parameter.py added to ya.make"
    ))
    
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} checks passed")
    
    if all(results):
        print("\nAll validation checks passed ✓")
        print("\nSummary of changes:")
        print("1. ✓ Added 'module' parameter to KikimrConfigGenerator")
        print("2. ✓ Added '--module' option handling in kikimr_runner.py")
        print("3. ✓ Created test_module_parameter.py test file")
        print("4. ✓ Added test to ya.make build file")
        return 0
    else:
        print("\nSome validation checks failed ✗")
        return 1


if __name__ == "__main__":
    sys.exit(main())


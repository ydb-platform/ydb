#!/usr/bin/env python3
"""Simple script to check YDB_QA_CONFIG status"""

import os
import sys

dir_path = os.path.dirname(__file__)
sys.path.insert(0, f"{dir_path}/analytics")

try:
    from ydb_wrapper import YDBWrapper
except Exception as e:
    print(f"⚠ Could not import ydb_wrapper: {e}")
    sys.exit(0)

try:
    print("Checking YDB configuration...")

    with YDBWrapper(silent=True) as wrapper:
        print(f"✓ Config: {wrapper.config_source}")

        if wrapper.check_credentials():
            print("✓ Credentials available")
            try:
                info = wrapper.get_cluster_info()
                print(f"✓ Connected  endpoint={info['endpoint']}  db={info['database']}")
            except Exception as e:
                print(f"⚠ Could not connect to YDB: {e}")
        else:
            print("⚠ Credentials not available (CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS missing)")

except Exception as e:
    print(f"⚠ Error: {e}")

sys.exit(0)

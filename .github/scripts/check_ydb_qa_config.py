#!/usr/bin/env python3
"""Simple script to check YDB_QA_CONFIG status"""

import os
import sys
import json

# Add analytics directory to path for ydb_wrapper import
dir_path = os.path.dirname(__file__)
sys.path.insert(0, f"{dir_path}/analytics")

# Wrap everything in try-except to ensure script never fails
try:
    try:
        from ydb_wrapper import YDBWrapper
    except Exception as e:
        print(f"⚠ Could not import ydb_wrapper: {e}")
        sys.exit(0)
    
    print("Checking YDB configuration...")
    
    try:
        # Try to create wrapper - it will load config and show logs
        with YDBWrapper(silent=False) as wrapper:
            if wrapper.check_credentials():
                print("✓ Credentials available")
                
                # Show config source
                if os.environ.get("YDB_QA_CONFIG"):
                    print("✓ Using YDB_QA_CONFIG from environment")
                    try:
                        config = json.loads(os.environ.get("YDB_QA_CONFIG"))
                        databases = list(config.get('databases', {}).keys())
                        if databases:
                            print(f"✓ Databases configured: {', '.join(databases)}")
                    except Exception:
                        pass
                else:
                    print("⚠ YDB_QA_CONFIG not set, using fallback config file")
                
                # Try to get cluster info (will show connection status)
                try:
                    cluster_info = wrapper.get_cluster_info()
                    if cluster_info:
                        print(f"✓ Connected to YDB cluster")
                except Exception as e:
                    print(f"⚠ Could not connect to YDB: {e}")
            else:
                print("⚠ Credentials not available (CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS missing)")
    except Exception as e:
        print(f"⚠ Error checking YDB config: {e}")
            
except Exception as e:
    print(f"⚠ Unexpected error: {e}")

# Always exit successfully - this script is informational only
sys.exit(0)


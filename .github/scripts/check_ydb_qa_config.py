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
        # Determine config file path (same logic as YDBWrapper)
        script_dir = os.path.dirname(__file__)
        config_file_path = f"{script_dir}/config/ydb_qa_config.json"
        
        # Check if config file exists
        if os.path.exists(config_file_path):
            print(f"✓ Using local config file: {config_file_path}")
            try:
                with open(config_file_path, 'r') as f:
                    config = json.load(f)
                databases = list(config.get('databases', {}).keys())
                if databases:
                    print(f"✓ Databases configured: {', '.join(databases)}")
            except Exception as e:
                print(f"⚠ Could not parse config file: {e}")
        else:
            print(f"⚠ Config file not found: {config_file_path}")
        
        # Note about YDB_QA_CONFIG (wrapper ignores it by default)
        if os.environ.get("YDB_QA_CONFIG"):
            print("ℹ YDB_QA_CONFIG is set, but wrapper uses local config file by default (use_local_config=True)")
        
        # Try to create wrapper - it will load config and show logs
        with YDBWrapper(silent=False) as wrapper:
            if wrapper.check_credentials():
                print("✓ Credentials available")
                
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


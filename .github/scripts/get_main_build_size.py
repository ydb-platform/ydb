#!/usr/bin/env python3

import os
import sys

# Добавляем путь к analytics скриптам
dir_path = os.path.dirname(__file__)
sys.path.insert(0, f"{dir_path}/analytics")

from ydb_wrapper import YDBWrapper

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")


def get_build_size(time_of_current_commit):
    try:
        # Используем silent режим, чтобы логи не попадали в stdout и не загрязняли результат
        with YDBWrapper(silent=True) as wrapper:
            if not wrapper.check_credentials():
                print("Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping", file=sys.stderr)
                return 0

            sql = f"""
            --!syntax_v1
            select git_commit_time,github_sha,size_bytes,size_stripped_bytes,build_preset
            from binary_size 
            where 
            github_workflow like "Postcommit%" and 
            github_ref_name="{branch}" and 
            build_preset="{build_preset}" and
            git_commit_time <= DateTime::FromSeconds({time_of_current_commit})
            order by git_commit_time desc
            limit 1;    
            """

            results = wrapper.execute_scan_query(sql)
            
            if results:
                row = results[0]
                main_data = {}
                for field in row:
                    main_data[field] = (
                        row[field]
                        if type(row[field]) != bytes
                        else row[field].decode("utf-8")
                    )
                
                return {
                    "github_sha": main_data["github_sha"],
                    "git_commit_time": str(main_data["git_commit_time"]),
                    "size_bytes": str(main_data["size_bytes"]),
                    "size_stripped_bytes": str(main_data["size_stripped_bytes"]),
                }
            else:
                print(
                    f"Error: Cant get binary size in db with params: github_workflow like 'Postcommit%', github_ref_name='{branch}', build_preset='{build_preset}, git_commit_time <= DateTime::FromSeconds({time_of_current_commit})'",
                    file=sys.stderr
                )
                return 0
    except Exception as e:
        print(f"Error: Failed to get main build size from YDB: {e}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    get_build_size()

#!/usr/bin/env python3

import json
import os
import subprocess


def get_ydbd_path():
    """Получает ydbd_path из конфига (env или файл)"""
    # Приоритет 1: YDB_QA_CONFIG из env
    ydb_qa_config_env = os.environ.get("YDB_QA_CONFIG")
    if ydb_qa_config_env:
        config_dict = json.loads(ydb_qa_config_env)
        return config_dict["variables"]["ydbd_path"]
    
    # Приоритет 2: локальный файл
    dir_path = os.path.dirname(__file__)
    config_path = f"{dir_path}/../config/ydb_qa_db.json"
    with open(config_path, 'r') as f:
        config_dict = json.load(f)
        return config_dict["variables"]["ydbd_path"]


def get_build_size():
    ydbd_path = get_ydbd_path()
    
    if not os.path.exists(ydbd_path):
        # can be possible due to incremental builds and ydbd itself is not affected by changes
        print("Error: {} not exists, skipping".format(ydbd_path))
        return 1

    binary_size_bytes = subprocess.check_output(
        ["bash", "-c", "cat {} | wc -c".format(ydbd_path)]
    )
    binary_size_stripped_bytes = subprocess.check_output(
        ["bash", "-c", "./ya tool strip {} -o - | wc -c".format(ydbd_path)]
    )

    size_stripped_bytes = int(binary_size_stripped_bytes.decode("utf-8"))
    size_bytes = int(binary_size_bytes.decode("utf-8"))
    if binary_size_bytes and binary_size_stripped_bytes:
        return {"size_bytes": size_bytes, "size_stripped_bytes": size_stripped_bytes}
    else:
        print(f"Error: Cant get build size")
        return 1


if __name__ == "__main__":
    get_build_size()

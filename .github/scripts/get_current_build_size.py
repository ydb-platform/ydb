#!/usr/bin/env python3

import configparser
import os
import subprocess
import sys


def get_ydbd_path():
    dir_path = os.path.dirname(__file__)
    config_path = f"{dir_path}/../config/variables.ini"
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Читаем из секции [YDBD] параметр YDBD_PATH
    return config.get("YDBD", "YDBD_PATH")


def get_build_size():
    ydbd_path = get_ydbd_path()
    
    if not os.path.exists(ydbd_path):
        # can be possible due to incremental builds and ydbd itself is not affected by changes
        print("Error: {} not exists, skipping".format(ydbd_path), file=sys.stderr)
        return 0

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
        print(f"Error: Cant get build size", file=sys.stderr)
        return 1


if __name__ == "__main__":
    get_build_size()

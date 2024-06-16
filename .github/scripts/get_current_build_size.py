#!/usr/bin/env python3

import configparser
import os
import subprocess


dir=os.path.dirname(__file__)
config = configparser.ConfigParser() 
config_file_path=f'{dir}/../config/ydb_qa_db.ini'
config.read(config_file_path)  

YDBD_PATH = config["YDBD"]["YDBD_PATH"]

def main():
  
    if not os.path.exists(YDBD_PATH):
        # can be possible due to incremental builds and ydbd itself is not affected by changes
        print("{} not exists, skipping".format(YDBD_PATH))
        #print("sizes:9168733686:707679377")
        return 0

    binary_size_bytes = subprocess.check_output(
        ["bash", "-c", "cat {} | wc -c".format(YDBD_PATH)]
    )
    binary_size_stripped_bytes = subprocess.check_output(
        ["bash", "-c", "./ya tool strip {} -o - | wc -c".format(YDBD_PATH)]
    )
   
    size_stripped_bytes=int(binary_size_stripped_bytes.decode("utf-8")),
    size_bytes=int(binary_size_bytes.decode("utf-8")),
    print(f'sizes:{size_bytes}:{size_stripped_bytes}')
    return 0

if __name__ == "__main__":
    exit(main())

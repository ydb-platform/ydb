#!/bin/bash
sudo apt update
sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev pkg-config -y
wget https://www.python.org/ftp/python/3.12.9/Python-3.12.9.tgz
tar -xf Python-3.12.9.tgz
cd Python-3.12.9
./configure --enable-optimizations
make -j $(nproc)
sudo make altinstall
python3.12 -m venv vector_index_bench
pip install --upgrade pip
pip install numpy scipy tqdm psycopg ydb
pip install "psycopg[binary]"
python3.12 db_benchmark.py --download
# python3.12 db_benchmark.py --backend ydb --dataset yfcc-10M --ydb-endpoint grpc://vla5-7705.search.yandex.net:2135  --ydb-database /Root/testdb
# python3.12 db_benchmark.py --backend ydb --dataset text2image-10M --ydb-endpoint grpc://vla5-7705.search.yandex.net:2135  --ydb-database /Root/testdb


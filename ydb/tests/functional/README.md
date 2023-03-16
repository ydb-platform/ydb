# How to run YDB functional tests

YDB function tests can be run via pytest. To launch them, complete the following steps:

0. Note that to run those tests, you will need Python version 3.10+ and Pytest version 7+.
1. Build YDB. You can use [this guide](https://github.com/ydb-platform/ydb/blob/main/BUILD.md).
2. Install `grpc-tools` package. You can use [this guide](https://grpc.io/docs/languages/python/quickstart).
3. Install some more packages:
    ```
    pip install PyHamcrest
    pip install tornado
    pip install xmltodict
    ```
4. Initialize the following enviroment variables:
    - `source_root` should match the root of YDB GitHub repo. If you did not change any of the commands from YDB
    build guide, then you should export the variable via
    ```
    export source_root=~/ydbwork/ydb
    ```
    - `build_root` should match the directory, where YDB was built. If you did not change any of the commands from YDB
    build guide, then you should export the variable via
    ```
    export build_root=~/ydbwork/build
    ```
5. Launch the script, which prepares the environment for YDB tests:
    ```
    source ${source_root}/ydb/tests/oss/prepare/prepare.sh
    ```
6. The script will put you inside directory with test sources, and you can run them:
    ```
    pytest -s
    ```

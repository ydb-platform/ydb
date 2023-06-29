# How to run YDB functional tests

YDB function tests can be run via pytest. To launch them, complete the following steps:

0. Note that to run those tests, you will need Python version 3.10+ and Pytest version 7+.
1. Build YDB. You can use [this guide](https://github.com/ydb-platform/ydb/blob/main/BUILD.md).
2. Install `grpc-tools` package. You can use [this guide](https://grpc.io/docs/languages/python/quickstart).
3. Install some more packages:
    ```bash
    pip install PyHamcrest
    pip install tornado
    pip install xmltodict
    ```
4. Initialize the following enviroment variables:
    - `source_root` should match the root of YDB GitHub repo. If you did not change any of the commands from YDB
    build guide, then you should export the variable via
    ```bash
    export source_root=~/ydbwork/ydb
    ```
    - `build_root` should match the directory, where YDB was built. If you did not change any of the commands from YDB
    build guide, then you should export the variable via
    ```bash
    export build_root=~/ydbwork/build
    ```
5. Launch the script, which prepares the environment:
    ```bash
    source ${source_root}/ydb/tests/oss/launch/prepare.sh
    ```
    Not that this script sets environment variables, so you need to re-run it, if terminal session is ended.
6. (Optional) Specify own YDBD binary file to use:
    ```bash
    export YDB_DRIVER_BINARY="path/to/ydbd/binary"
    ```
7. Launch tests:
    ```bash
    python ${source_root}/ydb/tests/oss/launch/launch.py --test-dir ${source_root}/ydb/tests/functional --xml-dir ${source_root}/ydb/tests/functional/test-results/xml
    ```
    Note that you can also run a specific suite via `--suite` argument.

    Alternatively, you can `cd` to `${source_root}/ydb/tests/functional` and invoke native `pytest`.
8. The script runs the tests. After that, you can see test report:
    ```bash
    cat ${source_root}/ydb/tests/functional/test-results/xml/res.xml
    ```

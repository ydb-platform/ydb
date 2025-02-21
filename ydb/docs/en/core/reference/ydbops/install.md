# Installing ydbops

{% include [warning.md](_includes/warning.md) %}

## Building from source

1. [Install Go](https://go.dev/doc/install). The minimal required Go version is 1.21.

2. Clone the `ydbops` repository from GitHub:

    ```bash
    git clone https://github.com/ydb-platform/ydbops.git
    ```

3. There are two ways to build `ydbops`:

- **Manual build.** Invoke `go build` in the repository root folder:

    ```bash
    go build
    ```

    The `ydbops` executable will be available in the repository root folder.

- **Automated build.** Invoke this command in the repository root folder:

  ```bash
  make build-in-docker
  ```

  The `ydbops` executables will be available in the `bin` folder. Binary files are generated for Linux and MacOS (arm64, amd64).

  | Binary name | Platform
  |-|-|
  | ydbops | Linux(amd64) |
  | ydbops_darwin_amd64 | MacOS(amd64) |
  | ydbops_darwin_arm64 | MacOS(arm64) |


4. To install the binary file, execute command `make`.

   Optional parameters:

    - `INSTALL_DIR`: The folder, to which the executable file will be installed. Default value: `~/ydb/bin`.

    - `BUILD_DIR`: The folder that contains the generated binary file. Use this parameter if you created the binary file manually. For example, use `BUILD_DIR=.` if the executable file is in the current working folder.

    ```bash
    make install [INSTALL_DIR=<path_to_install_folder>] [BUILD_DIR=<path_to_build_folder>]
    ```

    Sample command to install into `install_folder` from the current folder:

    ```bash
    make install INSTALL_DIR=install_folder BUILD_DIR=.
    ```

## Download the binary from releases page

You can download binary releases from [YDBOps Releases](../../downloads/index.md#ydbops).

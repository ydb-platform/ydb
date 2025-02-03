# Installing ydbops

{% include [warning.md](_includes/warning.md) %}

Currently, only building from source is available as an installation option. The pre-compiled executables will be available later.

## Building from source

1. [Install Go](https://go.dev/doc/install). The minimal required Go version is 1.21.

2. Clone the `ydbops` repository from GitHub:

    ```bash
    git clone https://github.com/ydb-platform/ydbops.git
    ```

3. There are two ways to build `ydbops`:

- Manual build. Invoke `go build` in the repository root folder:

    ```bash
    go build
    ```

    The `ydbops` executable will be available in the repository root folder.

- Automated build. Invoke this command in the repository root folder:

  ```bash
  make build-in-docker
  ```

  The `ydbops` executables will be available in the `bin` folder. Binary files are generated for Linux and MacOS (arm64, amd64).

4. To install the binary file, execute the following command:

   ```bash
   make install [INSTALL_DIR=<path_to_install_folder>] [BUILD_DIR=<path_to_build_folder>]
   ```

   You must replace source folder if you have created binary file manually:

   ```bash
   make install INSTALL_DIR=путь_к_каталогу BUILD_DIR=.
   ```

## Download the binary from releases page

You can download binary releases from official site - [YDBOps Releases](https://ydb.tech/docs/en/downloads/#ydbops).

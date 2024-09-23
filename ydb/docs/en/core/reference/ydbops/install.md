# Installing ydbops

{% include [warning.md](_includes/warning.md) %}

Currently, only building from source is available as an installation option. The pre-compiled executables will be available later.

### Building from source

1. [Install Go](https://go.dev/doc/install). The minimal required Go version is 1.21.

2. Clone the `ydbops` repository from GitHub:
    ```bash
    git clone https://github.com/ydb-platform/ydbops.git
    ```

3. Invoke `go build` in the repository root folder:
    ```bash
    go build
    ```
    The `ydbops` executable will be available in the repository root folder.

### Download the binary from releases page

{% note warning %}

This option has yet to be made available.

{% endnote %}

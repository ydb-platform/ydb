## Installing ydbops

{% note info %}

The article is being updated. Expect new content to appear and minor fixes to existing content.

{% endnote %}

Currently, only building from source is available as an installation option. The pre-compiled binaries will be available later.

### Building from source

1. [Install Go](https://go.dev/doc/install). The minimal required Go version is 1.21.

2. Clone the `ydbops` repository from Github:
    ```
    git clone https://github.com/ydb-platform/ydbops.git
    ```

3. Invoke `go build` in the repository root folder:
    ```
    go build
    ```
    The binary `ydbops` will be available in the repository root folder.

### Download the binary from releases page

{% note warning %}

This option has yet to be made available.

{% endnote %}

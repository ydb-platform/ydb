To install the {{ ydb-short-name }} DSTool, follow these steps:

1. Run the command:

    ```bash
    curl -sSL 'https://install.ydb.tech/dstool' | bash
    ```

    The script will install the {{ ydb-short-name }} DSTool. If the script is run from a `bash` or `zsh` shell, it will also add the `ydb-dstool` executable to the `PATH` environment variable. Otherwise, you can run it from the `~/ydb-dstool/bin` folder or add it to `PATH` manually.

1. To update the environment variables, restart the command shell.

1. Test it by running the command that shows cluster information:

    {% include [test step](./test.md) %}
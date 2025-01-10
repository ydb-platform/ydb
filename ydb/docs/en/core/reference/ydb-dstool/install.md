# Installing the {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux / macOS

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        ```bash
        curl -sSL 'https://install.ydb.tech/dstool' | bash
        ```

        The script will install the {{ ydb-short-name }} DSTool. If the script is run from a `bash` or `zsh` shell, it will also add the `ydb-dstool` executable to the `PATH` environment variable. Otherwise, you can run it from the `~/ydb-dstool/bin` folder or add it to `PATH` manually.

    1. To update the environment variables, restart the command shell.

    1. Test it by running the command that shows cluster information:

        {% include [test step](./_includes/test.md) %}

- Windows

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        - in **PowerShell**:

            ```powershell
            iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows1')
            ```

        - in **CMD**:

            ```cmd
            @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows'))"
            ```

    1. Specify whether to add `ydb-dstool` to the `PATH` environment variable:

        ```text
        Add ydb-dstool installation dir to your PATH? [Y/n]
        ```

    1. To update the environment variables, restart the command shell.

        {% note info %}

        The {{ ydb-short-name }} DSTool uses Unicode characters in the output of some commands. If these characters aren't displayed correctly in the Windows console, switch the encoding to UTF-8:

        ```cmd
        chcp 65001
        ```

        {% endnote %}

    1. Test it by running the command that shows cluster information:

        {% include [test step](./_includes/test.md) %}

{% endlist %}

# Installing the {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux

    {% include  [unix_install](./_includes/unix_install.md) %}

- macOS

    {% include  [unix_install](./_includes/unix_install.md) %}

- Windows

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        - in **PowerShell**:

            ```powershell
            iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows')
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

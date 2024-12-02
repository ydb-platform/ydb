# Installing the {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        ```bash
        curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
        ```

        The script will install the {{ ydb-short-name }} DSTool and add the executable file path to the `PATH` environment variable.

        {% note info %}

        The script will update the `PATH` variable only if you run it in the bash or zsh command shell. If you run the script in a different shell, add the path to the CLI to the `PATH` variable manually.

        {% endnote %}

    1. To update the environment variables, restart the command shell.

    1. Test it by running the command that shows cluster information:

        {% include [test step](./_includes/test.md) %}

- macOS

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        ```bash
        curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
        ```

        The script will install the {{ ydb-short-name }} DSTool and add the executable file path to the `PATH` environment variable.

    1. To update the environment variables, restart the command shell.

    1. Test it by running the command that shows cluster information:

        {% include [test step](./_includes/test.md) %}

- Windows

    To install the {{ ydb-short-name }} DSTool, follow these steps:

    1. Run the command:

        - in **PowerShell**:

            ```powershell
            iex (New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1')
            ```

        - in **CMD**:

            ```cmd
            @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1'))"
            ```

    1. Specify whether to add the executable file path to the `PATH` environment variable:

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

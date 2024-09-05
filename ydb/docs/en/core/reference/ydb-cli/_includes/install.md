# Installing the {{ ydb-short-name }} CLI

{% include [install_overlay.md](install_overlay.md) %}

{% list tabs %}

- Linux

  To install the {{ ydb-short-name }} CLI, run the command:

  ```bash
  curl -sSL {{ ydb-cli-install-url }} | bash
  ```

  The script will install the {{ ydb-short-name }} CLI and add the executable file path to the `PATH` environment variable.

  {% note info %}

  The script will update the `PATH` variable only if you run it in the bash or zsh command shell. If you run the script in a different shell, add the path to the CLI to the `PATH` variable yourself.

  {% endnote %}

  To update the environment variables, restart the command shell.

- macOS

  To install the {{ ydb-short-name }} CLI, run the command:

  ```bash
  curl -sSL {{ ydb-cli-install-url }} | bash
  ```

  The script will install the {{ ydb-short-name }} CLI and add the executable file path to the `PATH` environment variable.

  To update the environment variables, restart the command shell.

- Windows

  You can install the {{ ydb-short-name }} CLI using:

  **PowerShell.** To do this, run the command:

    ```powershell
    iex (New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb/install.ps1')
    ```

    Specify whether to add the executable file path to the `PATH` environment variable:

    ```text
    Add ydb installation dir to your PATH? [Y/n]
    ```

  **cmd.** To do this, run the command:

    ```cmd
    @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb/install.ps1'))"
    ```

    Specify whether to add the executable file path to the `PATH` environment variable:

    ```text
    Add ydb installation dir to your PATH? [Y/n]
    ```

    To update the environment variables, restart the command shell.

  {% note info %}

  The {{ ydb-short-name }} CLI uses Unicode characters in the output of some commands. If these characters aren't displayed correctly in the Windows console, switch the encoding to UTF-8:

  ```cmd
  chcp 65001
  ```

  {% endnote %}

{% endlist %}


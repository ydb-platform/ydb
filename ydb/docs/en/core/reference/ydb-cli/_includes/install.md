# Installing the {{ ydb-short-name }} CLI

<!-- markdownlint-disable blanks-around-fences -->

{% include [install_overlay.md](install_overlay.md) %}

{% list tabs %}

- Linux

  To install the {{ ydb-short-name }} CLI, run the command:

  ```bash
  curl -sSL {{ ydb-cli-install-url }} | bash
  ```

  The script will install the {{ ydb-short-name }} CLI and add the executable file path to the `PATH` environment variable. It will also generate shell completion files and print instructions on how to enable tab completion for commands and options.

  {% note info %}

  The script will update the `PATH` variable only if you run it in the bash or zsh command shell. If you run the script in a different shell, add the path to the CLI to the `PATH` variable yourself.

  Shell completion for bash requires the `bash-completion` package to be installed.

  {% endnote %}

  {% note tip %}

  If you use a non-standard shell configuration, you can source the generated completion files from any rc file. The files are located at `~/.local/share/ydb/completion.bash.inc` and `~/.local/share/ydb/completion.zsh.inc`, and are kept up to date automatically by `ydb update`.

  {% endnote %}

  To update the environment variables, restart the command shell.

- macOS

  To install the {{ ydb-short-name }} CLI, run the command:

  ```bash
  curl -sSL {{ ydb-cli-install-url }} | bash
  ```

  The script will install the {{ ydb-short-name }} CLI and add the executable file path to the `PATH` environment variable. It will also generate shell completion files and print instructions on how to enable tab completion for commands and options.

  To update the environment variables, restart the command shell.

- Windows

  You can install the {{ ydb-short-name }} CLI using:

  **PowerShell.** To do this, run the command:

    ```powershell
    iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/cli-windows')
    ```

    Specify whether to add the executable file path to the `PATH` environment variable:

    ```text
    Add ydb installation dir to your PATH? [Y/n]
    ```

  **cmd.** To do this, run the command:

    ```cmd
    @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/cli-windows'))"
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


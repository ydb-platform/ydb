# Установка {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux

     {% include  [unix_install](./_includes/unix_install.md) %}

- macOS

    {% include  [unix_install](./_includes/unix_install.md) %}

- Windows

    Чтобы установить {{ ydb-short-name }} DSTool:

    1. Выполните команду:

        - **PowerShell**:

            ```powershell
            iex (New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows')
            ```

        - **CMD**:

            ```cmd
            @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://install.ydb.tech/dstool-windows'))"
            ```

    1. Укажите, нужно ли добавить путь к `ydb-dstool` в переменную окружения `PATH`:

        ```text
        Add ydb-dstool installation dir to your PATH? [Y/n]
        ```

    1. Чтобы обновить переменные окружения, перезапустите командную оболочку.

        {% note info %}

        {{ ydb-short-name }} DSTool использует символы Юникода в выводе некоторых команд. При некорректном отображении таких символов в консоли Windows, переключите кодировку на UTF-8:

        ```cmd
        chcp 65001
        ```

        {% endnote %}

    1. Проверьте работу, выполнив команду вывода информации о кластере:

        {% include  [test step](./_includes/test.md) %}

{% endlist %}

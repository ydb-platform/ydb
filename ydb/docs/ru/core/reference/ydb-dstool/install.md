# Установка {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux

    Чтобы установить {{ ydb-short-name }} DSTool:

    1. Выполните команду:

        ```bash
        curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
        ```

        Скрипт установит {{ ydb-short-name }} DSTool и добавит путь к исполняемому файлу в переменную окружения `PATH`.

        {% note info %}

        Скрипт дополнит переменную `PATH`, только если его запустить в командной оболочке bash или zsh. Если вы запустили скрипт в другой оболочке, добавьте путь до CLI в переменную `PATH` самостоятельно.

        {% endnote %}

    1. Чтобы обновить переменные окружения, перезапустите командную оболочку.

    1. Проверьте работу, выполнив команду вывода информации о кластере:

       {% include  [test step](./_includes/test.md) %}

- macOS

    Чтобы установить {{ ydb-short-name }} DSTool:

    1. Выполните команду:

        ```bash
        curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
        ```

        Скрипт установит {{ ydb-short-name }} DSTool и добавит путь до исполняемого файла в переменную окружения `PATH`.

    1. Чтобы обновить переменные окружения, перезапустите командную оболочку.

    1. Проверьте работу, выполнив команду вывода информации о кластере:

        {% include  [test step](./_includes/test.md) %}

- Windows

    Чтобы установить {{ ydb-short-name }} DSTool:

    1. Выполните команду:

        - **PowerShell**:

            ```powershell
            iex (New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1')
            ```

        - **CMD**:

            ```cmd
            @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1'))"
            ```

    1. Укажите, нужно ли добавить путь к исполняемому в переменную окружения `PATH`:

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

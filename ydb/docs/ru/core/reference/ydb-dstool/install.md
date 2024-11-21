# Установка {{ ydb-short-name }} DSTool

<!-- markdownlint-disable blanks-around-fences -->

{% list tabs %}

- Linux

  Чтобы установить {{ ydb-short-name }} DSTool, выполните команду:

  ```bash
  curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
  ```

  Скрипт установит {{ ydb-short-name }} DSTool и добавит путь к исполняемому файлу в переменную окружения `PATH`.

  {% note info %}

  Скрипт дополнит переменную `PATH`, только если его запустить в командной оболочке bash или zsh. Если вы запустили скрипт в другой оболочке, добавьте путь до CLI в переменную `PATH` самостоятельно.

  {% endnote %}

  Чтобы обновить переменные окружения, перезапустите командную оболочку.

- macOS

  Чтобы установить {{ ydb-short-name }} DSTool, выполните команду:

  ```bash
  curl -sSL 'https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.sh' | bash
  ```

  Скрипт установит {{ ydb-short-name }} DSTool и добавит путь до исполняемого файла в переменную окружения `PATH`.

  Чтобы обновить переменные окружения, перезапустите командную оболочку.

- Windows

  {{ ydb-short-name }} DSTool можно установить с помощью:

  * PowerShell. Для этого выполните команду:

    ```powershell
    iex (New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1')
    ```

    Укажите, нужно ли добавить путь к исполняемому файлу в переменную окружения `PATH`:

    ```text
    Add ydb-dstool installation dir to your PATH? [Y/n]
    ```

  * cmd. Для этого выполните команду:

    ```cmd
    @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb-dstool/install.ps1'))"
    ```

    Укажите, нужно ли добавить путь к исполняемому в переменную окружения `PATH`:

    ```text
    Add ydb-dstool installation dir to your PATH? [Y/n]
    ```

    Чтобы обновить переменные окружения, перезапустите командную оболочку.

  {% note info %}

  {{ ydb-short-name }} DSTool использует символы Юникода в выводе некоторых команд. При некорректном отображении таких символов в консоли Windows, переключите кодировку на UTF-8:

  ```cmd
  chcp 65001
  ```

  {% endnote %}

{% endlist %}

Проверьте работу, выполнив команду вывода информации о кластере:

```bash
ydb-dstool -e <bs_endpoint> cluster list
```

* `bs_endpoint` — URI интерфейса управления распределенным хранилищем кластера {{ ydb-short-name }}. Интерфейс доступен на любом узле кластера по протоколу HTTP на порте 8765 по умолчанию. Пример URI: `http://localhost:8765`.

Результат:

```text
┌───────┬───────┬───────┬────────┬────────┬───────┬────────┐
│ Hosts │ Nodes │ Pools │ Groups │ VDisks │ Boxes │ PDisks │
├───────┼───────┼───────┼────────┼────────┼───────┼────────┤
│ 8     │ 16    │ 1     │ 5      │ 40     │ 1     │ 32     │
└───────┴───────┴───────┴────────┴────────┴───────┴────────┘
```

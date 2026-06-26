# Установка SDK

<!-- markdownlint-disable blanks-around-fences -->

Ниже приведены инструкции по быстрой установке SDK. На рабочей станции должны быть предварительно установлены и сконфигурированы инструменты по работе с выбранным языком программирования, а также пакетные менеджеры.

Описание порядка сборки из исходного кода размещено в репозиториях исходного кода на GitHub, ссылки на которые приведены на странице [{{ ydb-short-name }} SDK - Обзор](../index.md).

{% list tabs %}

- Go

  Выполните команду из командной строки:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  Для успешной установки в вашем окружении должен быть установлен [Go](https://go.dev/doc/install) версии не ниже 1.17.

- Java

  Добавьте зависимости в Maven-проект, как описано в пункте ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) файла `readme.md` в репозитории исходного кода.

- Python

  Выполните команду из командной строки:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  Если команда не выполнилась успешно, убедитесь, что в вашем окружении установлен [Python3](https://www.python.org/downloads/) версии 3.8 или более новой, со включенным  пакетным менеджером [pip](https://pypi.org/project/pip/).

- С#

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- JavaScript

  {% include [install/cmd_npm.md](install/cmd_npm.md) %}

  Минимальная поддерживаемая версия [Node.js®](https://nodejs.org/en/download) не ниже 20.19.

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

- C++

  ### Пакеты Debian (Ubuntu 24.04)

  Готовые `.deb`-пакеты для Ubuntu 24.04 (Noble), amd64 прикреплены к каждому [релизу на GitHub](https://github.com/ydb-platform/ydb-cpp-sdk/releases). Скачайте и установите их локально:

  {% include [install/cmd_cpp_deb.md](install/cmd_cpp_deb.md) %}

  Доступные пакеты:

  - `yandex-googleapis-api-common-protos` — обязательная зависимость protobuf;
  - `libydb-cpp-dev` — основной SDK: статическая библиотека, публичные заголовки и файлы CMake-пакета;
  - `libydb-cpp-iam-dev` — плагин IAM-аутентификации (опционально);
  - `libydb-cpp-otel-metrics-dev` — плагин метрик OpenTelemetry (опционально);
  - `libydb-cpp-otel-tracing-dev` — плагин трассировки OpenTelemetry (опционально; для заголовков и библиотек OTel требуется `libydb-cpp-otel-metrics-dev`).

  {% note info %}

  - Поддерживаемая платформа: Ubuntu 24.04 (Noble), amd64.
  - Пакеты собраны без apt-зависимостей; установите `yandex-googleapis-api-common-protos` вместе с основным пакетом SDK.
  - Для других платформ используйте инструкции по сборке из исходного кода ниже.

  {% endnote %}

  ### Использование SDK в CMake

  После установки пакетов подключите SDK в CMake-проекте:

  {% include [install/cmd_cpp_cmake.md](install/cmd_cpp_cmake.md) %}

  При конфигурации проекта передайте `-DCMAKE_PREFIX_PATH=/usr/share/yandex`, так как пакеты устанавливаются в префикс Yandex.

  ### Сборка из исходного кода

  Склонируйте репозиторий [ydb-cpp-sdk](https://github.com/ydb-platform/ydb-cpp-sdk) и выполните команду из командной строки:

  {% include [install/cmd_cpp_build.md](install/cmd_cpp_build.md) %}

  - `compiler` — ваш компилятор (`clang` или `gcc`);
  - `ydb_install_dir` — путь, по которому вы хотите установить SDK.

  Перед выполнением команды убедитесь, что все зависимости установлены. Полный список зависимостей для сборки и инструкции см. в [README](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/README.md) репозитория.

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

{% endlist %}

# Установка SDK

Ниже приведены инструкции по быстрой установке OpenSource SDK. На рабочей станции должны быть предварительно установлены и сконфигурированы инструменты по работе с выбранным языком программирования, а также пакетные менеджеры.

Описание порядка сборки из исходного кода размещено в репозиториях исходного кода на github.com, ссылки на которые приведены на странице [YDB SDK - Обзор](../index.md).

{% list tabs %}

- Python

  Выполните команду из командной строки:

  {% include [install/cmd_python.md](install/cmd_python.md) %}

  Если команда не выполнилась успешно, убедитесь, что в вашем окружении установлен [Python3](https://www.python.org/downloads/) версии 3.8 или более новой, со включенным  пакетным менеджером [pip](https://pypi.org/project/pip/).

- Go

  Выполните команду из командной строки:

  {% include [install/cmd_go.md](install/cmd_go.md) %}

  Для успешной установки в вашем окружении должен быть установлен [Go](https://go.dev/doc/install) версии не ниже 1.17.

- С# (.NET)

  {% include [install/cmd_dotnet.md](install/cmd_dotnet.md) %}

- Java

  Добавьте зависимости в Maven-проект, как описано в пункте ["Install the SDK"](https://github.com/ydb-platform/ydb-java-sdk#install-the-sdk) файла `readme.md` в репозитории исходного кода.

- PHP

  {% include [install/cmd_php.md](install/cmd_php.md) %}

- Node.JS

  {% include [install/cmd_nodejs.md](install/cmd_nodejs.md) %}

- Rust

  {% include [install/cmd_rust.md](install/cmd_rust.md) %}

{% endlist %}

# {{ ydb-short-name }} CLI - Начало работы

## Предварительные требования {#prerequisites}

Для выполнения команд через CLI вам потребуются параметры соединения с базой данных, которые вы можете получить при ее [создании](../create_db.md):

* [Эндпоинт](../../concepts/connect.md#endpoint)
* [Имя базы данных](../../concepts/connect.md#database)

Также вам может потребоваться токен или логин/пароль, если база данных требует [аутентификации](../auth.md). Для успешного исполнения сценария ниже вам нужно выбрать вариант их сохранения в переменной окружения.

## Установка CLI {#install}

Установите {{ ydb-short-name }} CLI, как описано в статье [Установка {{ ydb-short-name }} CLI](../../reference/ydb-cli/install.md).

Проверьте успешность установки YDB CLI запуском с параметром `--help`:

```bash
{{ ydb-cli }} --help
```

В ответ должно быть выведено приветственное сообщение, краткое описание синтаксиса, и перечень доступных команд:

```text
YDB client

Usage: ydb [options...] <subcommand>

Subcommands:
ydb
├─ config                   Manage YDB CLI configuration
│  └─ profile               Manage configuration profiles
│     ├─ activate           Activate specified configuration profile (aliases: set)
...
```

Все возможности работы со встроенной справкой {{ ydb-short-name }} CLI описаны в статье [Встроенная справка](../../reference/ydb-cli/commands/service.md#help) справочника по {{ ydb-short-name }} CLI.

## Проверьте успешность соединения {#ping} {#scheme-ls}

Для проверки успешности соединения можно использовать команду [получения перечня объектов](../../reference/ydb-cli/commands/scheme-ls.md) в базе данных `scheme ls`:

```bash
{{ ydb-cli }} -e <endpoint> -d <database> scheme ls
```

При успешном выполнении команды в ответ будет выведен перечень объектов в базе данных. Если вы еще не создавали ничего в БД, то вывод будет содержать только системные каталоги `.sys` и `.sys_health`, в которых находятся [диагностические представления YDB](../../troubleshooting/system_views_db.md).

{% include [cli/ls_examples.md](cli/ls_examples.md) %}

## Создание профиля соединения {#profile}

Чтобы не писать параметры соединения каждый раз при вызове YDB CLI, воспользуйтесь [профилем](../../reference/ydb-cli/profile/index.md). Создание предложенного ниже профиля позволит вам также копировать дальнейшие команды через буфер обмена без их редактирования, вне зависимости от того, на какой базе данных вы проходите сценарий начала работы.

[Создайте профиль](../../reference/ydb-cli/profile/create.md) `quickstart` следующей командой:

```bash
{{ ydb-cli }} config profile create quickstart -e <endpoint> -d <database>
```

В качестве параметров используйте проверенные на [предыдущем шаге](#ping) значения. Например, для создания профиля соединения с локальной базой данных YDB, созданной по сценарию самостоятельного развертывания [в Docker](../self_hosted/ydb_docker.md), выполните следующую команду:

```bash
{{ ydb-cli }} config profile create quickstart -e grpc://localhost:2136 -d /local
```

Проверьте работоспособность профиля командой `scheme ls`:

```bash
{{ ydb-cli }} -p quickstart scheme ls
```

## Исполнение YQL скрипта {#yql}

Команда {{ ydb-short-name }} CLI `yql` позволяет выполнить любую команду (как DDL, так и DML) на [языке YQL](../../yql/reference/index.md) - диалекте SQL, поддерживаемом {{ ydb-short-name }}:

```bash
{{ ydb-cli }} -p <profile_name> yql -s <yql_request>
```

Например:

* Создание таблицы:

  ```bash
  {{ ydb-cli }} -p quickstart yql -s "create table t1( id uint64, primary key(id))"
  ```

* Добавление записи:

  ```bash
  {{ ydb-cli }} -p quickstart yql -s "insert into t1(id) values (1)"
  ```

* Выборка данных:

  ```bash
  {{ ydb-cli }} -p quickstart yql -s "select * from t1"
  ```

Если вы получаете ошибку `Profile quickstart does not exist`, значит вы не создали его на [предыдущем шаге](#profile).

## Специализированные команды CLI {#ydb-api}

Исполнение команд через `ydb yql` является хорошим простым способом начать работу. Однако, YQL-интерфейс поддерживает неполный набор возможных функций, доступных на YDB API, а также работает не самым эффективным образом из-за своей универсальности.

YDB CLI поддерживает отдельные команды с полным набором опций для всех существующих YDB API. Описание полного перечня команд находится в [справочнике по YDB CLI](../../reference/ydb-cli/index.md).

## Продолжение знакомства с YDB {#next}

Перейдите к статье [YQL - Начало работы](../yql.md) для продолжения знакомства с YDB.

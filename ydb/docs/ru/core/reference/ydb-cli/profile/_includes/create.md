# Создание и изменение профиля

Значения параметров соединения для создаваемого или изменяемого профиля могут быть:
- Заданы [в командной строке](#cmdline)
- Запрошены [в интерактивном режиме](#interactive) из консоли

## Командная строка {#cmdline}

Для создания или изменения профиля из командной строки применяется следующая команда:

``` bash
{{ ydb-cli }} config profile create <profile_name> <connection_options>
```

В данной команде:
- `<profile_name>` -- обязательное имя профиля
- `<connection options>` -- [параметры соединения](../../connect.md#command-line-pars) для записи в профиле. Необходимо указание как минимум одного параметра соединения, иначе команда будет выполняться в [интерактивном режиме](#interactive).

Если профиль с указанным именем существует, то в нем будут обновлены те параметры, значения которых переданы в командной строке. Значения не перечисленных в командной строке параметров останутся без изменений.

Используются только те значения, которые непосредственно указаны в командной строке, без обращений к переменным окружения или активированному профилю.

### Примеры {#cmdline-examples}

#### Создание профиля по ранее использованным параметрам соединения {#cmdline-example-from-explicit}

Любая команда выполнения операции в базе данных YDB с явно заданными параметрами соединения может быть преобразована в команду создания профиля перемещением параметров соединения из глобальных опций в опции команды `config profile create`.

Например, если вы успешно выполнили команду `scheme ls` со следующими реквизитами:

```bash
{{ydb-cli}} \
  -e grpcs://example.com:2135 -d /Root/somedatabase --sa-key-file ~/sa_key.json \
  scheme ls
```

То создать профиль для соединения с использованной базой данных можно следующей командой:

```bash
{{ydb-cli}} \
  config profile create db1 \
  -e grpcs://example.com:2135 -d /Root/somedatabase --sa-key-file ~/sa_key.json
```

Теперь можно записать исходную команду гораздо короче:

```bash
{{ydb-cli}} -p db1 scheme ls
```

#### Профиль для соединения с локальной базой данных {#cmdline-example-local}

Создание профиля `local` для соединения с локальной БД YDB, развернутой сценариями быстрого развертывания из [бинарного файла](../../../../getting_started/self_hosted/ydb_local.md) или [в Docker](../../../../getting_started/self_hosted/ydb_docker.md):

```bash
{{ydb-cli}} config profile create local --endpoint grpc://localhost:2136 --database /Root/test
```

Определение способа аутентификации по логину и паролю в профиле `local`:

```bash
{{ydb-cli}} config profile create local --user user1 --password-file ~/pwd.txt
```

## Интерактивный режим {#interactive}

Профили создаются и изменяются в интерактивном режиме следующими командами:

``` bash
{{ ydb-cli }} init
```

или

``` bash
{{ ydb-cli }} config profile create [profile_name] [connection_options]
```

В данной команде:
- `[profile_name]` -- необязательное имя создаваемого или изменяемого профиля
- `[connection_options]` -- необязательные [параметры соединения](../../connect.md#command-line-pars) для записи в профиле

Команда `init` всегда работает в интерактивном режиме, а `config profile create` запускается в интерактивном режиме в случае, если не указано имя профиля, или не указан ни один из параметров соединения в командной строке.

Начало интерактивного сценария отличается в командах `init` и `profile create`:

{% list tabs %}

- Init

  1. Выводится перечень существующих профилей (если они есть), и предлагается выбор - создать новый (Create a new) или изменить конфигурацию одного из существующих:

     ```text
     Please choose profile to configure:
     [1] Create a new profile
     [2] test
     [3] local
     ```

  2. Если существующих профилей нет, или выбран вариант `1` на предыдущем шаге, то запрашивается имя профиля для создания:

     ``` text
     Please enter name for a new profile: 
     ```

  3. Если ввести в этот момент имя существующего профиля, то {{ ydb-short-name }} CLI переходит к шагам изменения его параметров, как если бы сразу была выбрана опция с именем этого профиля.

- Profile Create

   Если в командной строке не указано имя профиля, то оно запрашивается:
   ```text
   Please enter configuration profile name to create or re-configure:
   ```

{% endlist %}

Далее будет последовательно предложено выполнить действия с каждым параметром соединения, доступным для сохранения в профиле:

- Не сохранять в профиле - Don't save
- Задать или выбрать значение - Set a new value или Use <вариант>
- Оставить предыдущее значение - Use current value (опция доступна при изменении существующего профиля)

### Пример {#interactive-example}

Создание нового профиля `mydb1`:

1. Выполните команду:

    ```bash
    {{ ydb-cli }} config profile create mydb1
    ```

1. Введите [эндпоинт](../../../../concepts/connect.md#endpoint) или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure endpoint:
     [1] Set a new endpoint value
     [2] Don't save endpoint for profile "mydb1"
    Please enter your numeric choice: 
    ```

1. Введите [имя базы данных](../../../../concepts/connect.md#database) или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure database:
     [1] Set a new database value
     [2] Don't save database for profile "mydb1"
    Please enter your numeric choice: 
    ```

1. Выберите режим аутентификации или не сохраняйте этот параметр для профиля:

    ```text
    Pick desired action to configure authentication method:
      [1] Use static credentials (user & password)
      [2] Use IAM token (iam-token) cloud.yandex.ru/docs/iam/concepts/authorization/iam-token
      [3] Use OAuth token of a Yandex Passport user (yc-token). Doesn't work with federative accounts. cloud.yandex.ru/docs/iam/concepts/authorization/oauth-token
      [4] Use metadata service on a virtual machine (use-metadata-credentials) cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm
      [5] Use service account key file (sa-key-file) cloud.yandex.ru/docs/iam/operations/iam-token/create-for-sa
      [6] Set new OAuth token (ydb-token)
      [7] Don't save authentication data for profile "mydb1"
    Please enter your numeric choice:
    ```

    Если вы не уверены какой режим аутентификации выбрать, воспользуйтесь рецептом из статьи [Аутентификация](../../../../getting_started/auth.md) в разделе "Начало работы".

    Все доступные методы аутентификации описаны в статье [{#T}](../../../../concepts/auth.md). Набор методов и текст подсказок может отличаться от приведенного в данном примере.

    Если выбранный вами метод подразумевает указание дополнительного параметра, вам будет предложено его ввести. Например, если вы выбрали `4` (Use service account key file):

    ```text
    Please enter Path to service account key file (sa-key-file): 
    ```

1. На последнем шаге вам будет предложено активировать созданный профиль для использования по умолчанию. Ответьте 'n' (Нет), пока вы не прочитали статью про [Активацию профиля и применение активированного профиля](../activate.md):

    ```text
    Activate profile "mydb1" to use by default? (current active profile is not set) y/n: n
    ```

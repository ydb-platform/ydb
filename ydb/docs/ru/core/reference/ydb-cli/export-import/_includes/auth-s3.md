# Соединение с S3-совместимыми объектными хранилищами

Команды выгрузки и загрузки из S3-совместимых хранилищ `export s3` и `import s3` используют одинаковые параметры для соединения с S3 и аутентификации. О том, как узнать эти параметры для некоторых облачных поставщиков, описано в разделе [Получение параметров соединения с S3](#procure) ниже.

## Соединение {#conn}

Соединение с S3 требует указания эндпоинта и бакета:

`--s3-endpoint HOST`: Эндпоинт S3. `HOST` - валидное имя хоста, например `storage.yandexcloud.net`

`--bucket STR`: Бакет S3. `STR` - строка, содержащая имя бакета

## Аутентификация {#auth}

Для успешного соединения, кроме случая загрузки из публично доступного бакета, потребуется аутентифицироваться под учетной записью, для которой разрешена запись (для выгрузки) или чтение (для загрузки) из данного бакета.

Для аутентификации в S3 необходимы два параметра:

- Идентификатор ключа доступа (access_key_id).
- Секретный ключ доступа (secret_access_key).

{{ ydb-short-name }} CLI определяет значения этих параметров из следующих источников (в порядке убывания приоритета):

1. Командной строки.
2. Переменных окружения.
3. Файла `~/.aws/credentials`.

### Параметры командной строки

* `--access-key` — идентификатор ключа доступа.
* `--secret-key` — секретный ключ доступа.
* `--aws-profile` — имя профиля в файле `~/.aws/credentials`. Значение по умолчанию `default`.

### Переменные окружения

Если какой-либо параметр аутентификации не указан в командной строке, {{ ydb-short-name }} CLI пробует его получить из следующих переменных окружения:

* `AWS_ACCESS_KEY_ID` — идентификатор ключа доступа.
* `AWS_SECRET_ACCESS_KEY` — секретный ключ доступа.
* `AWS_PROFILE` — имя профиля в файле `~/.aws/credentials`.

### Файл аутентификации AWS

Если какой-либо параметр аутентификации не указан в командной строке и его не удалось получить из переменной окружения, {{ ydb-short-name }} CLI пробует его получить из указанного профиля или профиля по умолчанию в файле `~/.aws/credentials`, применяемого для аутентификации [AWS CLI](https://aws.amazon.com/ru/cli/). Данный файл может быть создан командой AWS CLI `aws configure`.

## Получение параметров соединения с S3 {#procure}

### {{ yandex-cloud }}

Ниже описан сценарий получения ключей доступа к [{{ yandex-cloud }} Object Storage](https://cloud.yandex.ru/docs/storage/) с применением {{ yandex-cloud }} CLI.

1. [Установите и сконфигурируйте](https://cloud.yandex.ru/docs/cli/quickstart) {{ yandex-cloud }} CLI.

2. Получите ID вашего каталога в облаке следующей командой, его понадобится указывать в командах ниже:

   ```bash
   yc config list
   ```

   В выводе идентификатор каталога в облаке находится в строке `folder-id:`:

   ```yaml
   folder-id: b2ge70qdcff4bo9q6t19
   ```


3. [Создайте сервисный аккаунт](https://cloud.yandex.ru/docs/iam/operations/sa/create), выполнив следующую команду:

   ```bash
   yc iam service-account create --name s3account
   ```

   Вы можете указать любое имя аккаунта кроме `s3account` или использовать существующий, тогда вам понадобится его также заменять при копировании команд ниже через буфер обмена.

4. [Назначьте сервисному аккаунту](https://cloud.yandex.ru/docs/iam/operations/sa/assign-role-for-sa) роли в соответствии с необходимым уровнем доступа к S3, выполнив команду:

   {% list tabs %}

   - Чтение (для загрузки в базу данных {{ ydb-short-name }})

     ```bash
     yc resource-manager folder add-access-binding <folder-id> \
       --role storage.viewer --subject serviceAccount:s3account
     ```

   - Запись (для выгрузки из базы данных {{ ydb-short-name }})

     ```bash
     yc resource-manager folder add-access-binding <folder-id> \
       --role storage.editor --subject serviceAccount:s3account
     ```

   {% endlist %}

   , где `<folder-id>` - это идентификатор каталога в облаке, полученный на шаге 2.

   Вы можете также ознакомиться с [полным перечнем](https://cloud.yandex.ru/docs/iam/concepts/access-control/roles#object-storage) ролей {{ yandex-cloud }}.

5. Получите [статические ключи доступа](https://cloud.yandex.ru/docs/iam/operations/sa/create-access-key), выполнив следующую команду:

   ```bash
   yc iam access-key create --service-account-name s3account
   ```

   Успешно исполненная команда выведет информацию об атрибутах access_key и значение secret:

   ```yaml
   access_key:
     id: aje6t3vsbj8lp9r4vk2u
     service_account_id: ajepg0mjt06siuj65usm
     created_at: "2018-11-22T14:37:51Z"
     key_id: 0n8X6WY6S24N7OjXQ0YQ
   secret: JyTRFdqw8t1kh2-OJNz4JX5ZTz9Dj1rI9hxtzMP1
   ```

   В данном выводе:
   - `access_key.key_id` - это идентификатор ключа доступа
   - `secret` - это секретный ключ доступа

{% include [s3_conn_procure_overlay.md](s3_conn_procure_overlay.md) %}

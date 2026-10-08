# Чтение данных из Object Storage с делегированием IAM

В этом руководстве вы настроите делегирование сервисного аккаунта, создадите [секрет с делегированием IAM](../concepts/datamodel/iam-delegation-secrets.md) и прочитаете CSV-файл из закрытого бакета Yandex Object Storage [федеративным запросом](../concepts/query_execution/federated_query/index.md). {{ ydb-short-name }} будет получать и обновлять IAM-токены самостоятельно: создавать статический ключ или записывать токен в секрет не нужно.

{% note info %}

Руководство описывает сценарий для предстоящего выпуска секретов с делегированием IAM. Он требует поддержки `SOURCE="IAM_DELEGATION"` и аутентификации `AUTH_METHOD="TOKEN"` для источника `ObjectStorage`. Поддержка токенов в этом источнике появится вместе с новыми секретами. Включение сервиса в IAM само по себе не включает эти возможности в базе.

{% endnote %}

## Перед началом

Подготовьте базу Managed Service for YDB с поддержкой федеративных запросов и указанной выше функциональности. Установите и настройте [Yandex Cloud CLI](https://yandex.cloud/ru/docs/cli/quickstart) (`yc`) и [YDB CLI](../reference/ydb-cli/install.md) (`ydb`).

Настройку облачных прав выполняет администратор соответствующих ресурсов. SQL-команды выполняет пользователь, которому вы выдадите права ниже. Для примера используйте закрытый бакет без шифрования ключом KMS; для зашифрованного бакета сервисному аккаунту дополнительно понадобятся права на ключ.

## 1. Разрешите делегирование в облаке {#enable-delegation}

Проверьте [статус сервиса](https://yandex.cloud/ru/docs/iam/operations/service-control/list-get) `ydb` в облаке, которому принадлежит делегируемый сервисный аккаунт:

```bash
yc iam service-control get ydb --cloud-id "<service_account_cloud_id>"
```

Здесь и далее `<service_account_cloud_id>` обозначает идентификатор облака сервисного аккаунта. Если аккаунт ещё не создан, используйте облако каталога, в котором создадите его на следующем шаге. Это не обязательно облако базы YDB или бакета.

При статусе `DISABLED` попросите администратора или владельца облака [включить сервис](https://yandex.cloud/ru/docs/iam/operations/service-control/enable-disable):

```bash
yc iam service-control enable ydb --cloud-id "<service_account_cloud_id>"
```

Повторите проверку: ожидается статус `ENABLED`. При исходном статусе `ENABLED` дополнительных действий не требуется; `DEFAULT` допускает автоматическое включение при первом делегировании. Если сервис `ydb` не найден, уточните доступность функции в [поддержке {{ yandex-cloud }}](https://yandex.cloud/ru/support).

## 2. Создайте сервисный аккаунт {#service-account}

[Создайте сервисный аккаунт](https://yandex.cloud/ru/docs/iam/operations/sa/create) для чтения данных или используйте существующий:

```bash
yc iam service-account create --name "s3-reader" --folder-id "<service_account_folder_id>"
```

Вместо `<service_account_folder_id>` укажите каталог в облаке из шага 1. Сохраните поле `id` из результата команды: далее оно обозначено как `<service_account_id>`.

## 3. Подготовьте бакет и данные {#prepare-bucket}

[Создайте закрытый бакет](https://yandex.cloud/ru/docs/storage/operations/buckets/create) или выберите существующий. Далее `<bucket_name>` обозначает его имя. Публичный доступ к объектам и их списку для этого сценария не нужен.

Выдайте сервисному аккаунту из шага 2 роль `storage.viewer` **на этот бакет**. Она разрешает читать объекты и их список, но не изменять данные. В [консоли {{ yandex-cloud }}](https://console.yandex.cloud/) откройте бакет, перейдите в **Безопасность → Права доступа**, нажмите **Назначить роли**, выберите сервисный аккаунт и роль `storage.viewer`, затем сохраните изменения. Подробнее: [права на бакет](https://yandex.cloud/ru/docs/storage/operations/buckets/iam-access) и [роли Object Storage](https://yandex.cloud/ru/docs/storage/security/).

Создайте локальный файл `sales.csv` в кодировке UTF-8:

```csv
product,amount
book,1200
pen,150
notebook,450
```

[Загрузите файл](https://yandex.cloud/ru/docs/storage/operations/objects/upload) в бакет под ключом `iam-delegation-example/sales.csv`:

```bash
yc storage s3api put-object \
  --bucket "<bucket_name>" \
  --key "iam-delegation-example/sales.csv" \
  --body "./sales.csv"
```

Команду загрузки выполняет пользователь с правом записи в бакет, а не созданный аккаунт с правом только на чтение. Выберите новый ключ для примера, если по указанному ключу уже есть нужные вам данные: загрузка заменяет объект с тем же ключом. Если выбрали другой ключ, укажите его также в запросе на шаге 8.

## 4. Разрешите пользователю делегировать аккаунт {#caller-permissions}

Пользователю, который выполнит `CREATE SECRET`, нужна роль `iam.serviceAccounts.user` на сервисный аккаунт. Это отдельное право: роль аккаунта на бакет его не заменяет.

```bash
yc iam service-account add-access-binding --id "<service_account_id>" \
  --role iam.serviceAccounts.user --subject "<subject_type>:<subject_id>"
```

Вместо `<subject_type>:<subject_id>` укажите исполнителя SQL-команды: `userAccount:<идентификатор>` для аккаунта Яндекса, `federatedUser:<идентификатор>` для федеративного пользователя или `serviceAccount:<идентификатор>` для сервисного аккаунта. Подробнее: [назначение прав на сервисный аккаунт](https://yandex.cloud/ru/docs/iam/operations/sa/set-access-bindings).

В базе YDB этому пользователю нужны права на создание секретов и внешних источников в выбранной директории. Для чтения данных через источник также нужны права на его использование и `SELECT ROW` на секрет. Создатель секрета получает права на него автоматически; если запросы будет выполнять другой пользователь, выдайте ему доступ по разделу [Управление доступом к секретам](../concepts/datamodel/secrets.md#secret_access).

## 5. Подключитесь к базе YDB {#connect}

В консоли {{ yandex-cloud }} откройте базу YDB и скопируйте её эндпоинт и путь из параметров подключения. Создайте [профиль YDB CLI](../reference/ydb-cli/profile/create.md):

```bash
ydb config profile create "delegation-guide"
```

Укажите эндпоинт с `grpcs://`, путь базы и [аутентификацию через IAM](../reference/ydb-cli/connect.md) от имени пользователя из шага 4. Локальный логин базы для создания секрета с делегированием не подходит. Профили `yc` и `ydb` независимы: вход в `yc` не настраивает аутентификацию `ydb`.

Если для входа в YDB CLI вы выбрали IAM-токен, получите его для этого пользователя по [инструкции IAM](https://yandex.cloud/ru/docs/iam/operations/iam-token/create). Это учётные данные клиента YDB, а не токен сервисного аккаунта для доступа к бакету. В секрет этот токен записывать не нужно.

Все дальнейшие SQL-команды выполняйте в этой базе. Имена `s3_reader` и `s3_data` должны быть свободны.

## 6. Создайте секрет с делегированием {#create-secret}

Сохраните следующий SQL в файл `create-secret.sql`, подставив идентификаторы сервисного аккаунта и его облака:

```yql
CREATE SECRET s3_reader WITH (
    SOURCE="IAM_DELEGATION",
    SERVICE_ACCOUNT_ID="<service_account_id>",
    RESOURCE="<service_account_cloud_id>"
);
```

`RESOURCE` обозначает облако сервисного аккаунта из шага 1, а не имя бакета, его каталог или путь базы YDB.

Выполните файл командой [YDB CLI](../reference/ydb-cli/sql.md):

```bash
ydb -p "delegation-guide" sql -f "create-secret.sql"
```

Дождитесь успешного завершения команды, прежде чем переходить к следующему шагу. {{ ydb-short-name }} настроит делегирование от имени пользователя, выполнившего команду.

## 7. Создайте внешний источник данных {#create-source}

Сохраните следующий SQL в файл `create-source.sql`. Вместо `<bucket_name>` укажите имя бакета из шага 3:

```yql
CREATE EXTERNAL DATA SOURCE s3_data WITH (
    SOURCE_TYPE="ObjectStorage",
    LOCATION="https://storage.yandexcloud.net/<bucket_name>/",
    AUTH_METHOD="TOKEN",
    TOKEN_SECRET_PATH="s3_reader"
);
```

`TOKEN_SECRET_PATH` ссылается на секрет из предыдущего шага. В примере секрет и источник создаются в корне базы; для другой директории укажите полный путь к секрету.

```bash
ydb -p "delegation-guide" sql -f "create-source.sql"
```

Создание источника ещё не подтверждает доступ к объектам бакета. Проверьте его запросом на следующем шаге.

## 8. Прочитайте файл федеративным запросом {#read-data}

Сохраните запрос в файл `read-sales.sql`:

```yql
SELECT
    product,
    amount
FROM s3_data.`iam-delegation-example/sales.csv`
WITH (
    FORMAT="csv_with_names",
    SCHEMA=(
        product Utf8 NOT NULL,
        amount Uint64 NOT NULL
    )
)
WHERE amount >= 400
ORDER BY amount DESC;
```

Путь после `s3_data.` отсчитывается от корня бакета, указанного в `LOCATION`. Формат `csv_with_names` означает CSV с названиями колонок в первой строке.

```bash
ydb -p "delegation-guide" sql -f "read-sales.sql"
```

Для файла из шага 3 запрос вернёт две строки:

| product | amount |
| --- | --- |
| book | 1200 |
| notebook | 450 |

Данные читаются из Object Storage, без предварительного копирования в таблицу YDB. Для доступа к бакету {{ ydb-short-name }} использует IAM-токен сервисного аккаунта `s3-reader` и обновляет его самостоятельно.

Если запрос возвращает ошибку доступа, проверьте роль сервисного аккаунта на бакет и права пользователя на источник и секрет. Если объект не найден, проверьте имя бакета и ключ загруженного файла. Другие форматы и способы чтения описаны в разделе [Чтение из бакетов S3 через внешние источники данных](../concepts/query_execution/federated_query/s3/external_data_source.md).

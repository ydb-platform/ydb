### Выгрузка базы данных {#example-full-db}

Выгрузка всех несистемных объектов базы данных в директорию `export1` в бакете `mybucket` с использованием параметров аутентификации S3 из переменных окружения или файла `~/.aws/credentials`:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --destination-prefix export1
```

### Выгрузка нескольких директорий {#example-specific-dirs}

Выгрузка объектов из директорий `dir1` и `dir2` базы данных, в директорию `export1` в бакете `mybucket`, с использованием явно заданных параметров аутентификации в S3:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --destination-prefix export1 --include dir1 --include dir2
```

Либо с использованием альтернативного способа:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Выгрузка с шифрованием {#example-encryption}

Выгрузка всей базы данных с шифрованием:
- С использованием алгоритма шифрования `AES-128-GCM`
- С генерацией случайного ключа утилитой `openssl` в файл `~/my_secret_key`
- С чтением сгенерированного ключа из файла `~/my_secret_key`
- В префикс пути `export1` в бакете S3 `mybucket`
- С использованием параметров аутентификации S3 из переменных окружения или файла `~/.aws/credentials`

```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```

Выгрузка директории `dir1` базы данных с шифрованием:
- С использованием алгоритма шифрования `AES-256-GCM`
- С генерацией случайного ключа утилитой `openssl` в переменную окружения `YDB_ENCRYPTION_KEY`
- С чтением сгенерированного ключа из переменной окружения `YDB_ENCRYPTION_KEY`
- В префикс пути `export1` в бакете S3 `mybucket`
- С использованием параметров аутентификации S3 из переменных окружения или файла `~/.aws/credentials`

```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export s3 \
  --root-path dir1 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-256-GCM
```

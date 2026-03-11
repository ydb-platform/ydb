### Загрузка в корень базы данных {#example-full-db}

Загрузка в корень базы данных содержимого директории `export1` в бакете `mybucket` с использованием параметров аутентификации S3 из переменных окружения или файла `~/.aws/credentials`:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --source-prefix export1
```

### Загрузка нескольких директорий {#example-specific-dirs}

Загрузка объектов из директорий `dir1` и `dir2` выгрузки, которая находится в директории `export1` в бакете `mybucket`, в одноименные директории базы данных с использованием явно заданных параметров аутентификации в S3:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --include dir1 --include dir2
```

### Перечисление объектов в существующей зашифрованной выгрузке {#example-list}

Перечисление путей всех объектов в существующей зашифрованной выгрузке, которая находится в директории `export1` в бакете `mybucket`, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --encryption-key-file ~/my_secret_key
  --list
```

### Загрузка зашифрованной выгрузки {#example-encryption}

Загрузка одной таблицы, которая была выгружена по пути `dir/my_table`, в путь `dir1/dir/my_table` из зашифрованной выгрузки, расположенной по префиксу `export1` в бакете `mybucket`, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```

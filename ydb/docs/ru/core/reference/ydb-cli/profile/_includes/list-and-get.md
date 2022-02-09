# Получение информации о профиле

## Получение списка профилей {#list}

Получение списка профилей:

```bash
{{ ydb-cli }} config profile list
```

Если существует текущий [активированный профиль](../activate.md), он будет помечен как `(active)` в выведенном списке, например:

``` text
prod
test (active)
local
```

## Получение подробной информации о профиле {#get}

Получение параметров, сохраненных в заданном профиле:

```bash
{{ ydb-cli }} config profile get <profile_name>
```

Например:

```bash
$ {{ ydb-cli }} config profile get local1
  endpoint: grpcs://ydb.serverless.yandexcloud.net:2135
  database: /rul1/b1g8skp/etn02099
  sa-key-file: /Users/username/secrets/sa_key_test.json
```

## Получение профилей с содержимым {#get-all}

Полная информация по всем профилям и сохраненным в них параметрам:

```bash
{{ ydb-cli }} config profile list --with-content
```

Вывод данной команды объединяет вывод команды получения списка (с пометкой активного профиля) и параметров каждого профиля в следующих строках после его имени.


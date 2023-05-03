# Список эндпоинтов

Информационная команда `discovery list` позволяет получить список [эндпоинтов](../../../../concepts/connect.md#endpoint) кластера {{ ydb-short-name }}, с которыми можно открывать соединения для работы с заданной базой данных:

``` bash
{{ ydb-cli }} [connection options] discovery list
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

В выводимых в ответ строках содержится следующая информация:
1. Эндпоинт, включая протокол и порт
2. В квадратных скобках зона доступности
3. С символом `#` перечень сервисов {{ ydb-short-name }}, доступных на данном эндпоинте

Запрос к кластеру {{ ydb-short-name }} на endpoint discovery выполняется в {{ ydb-short-name }} SDK при инициализации драйвера, и команду CLI `discovery list` можно использовать для локализации возможных проблем соединения.

## Пример

``` bash
$ ydb -p quickstart discovery list
grpcs://vm-etn01q5-ysor.etn01q5k.ydb.mdb.yandexcloud.net:2135 [sas] #table_service #scripting #discovery #rate_limiter #locking #kesus
grpcs://vm-etn01q5-arum.etn01ftr.ydb.mdb.yandexcloud.net:2135 [vla] #table_service #scripting #discovery #rate_limiter #locking #kesus
grpcs://vm-etn01q5beftr.ydb.mdb.yandexcloud.net:2135 [myt] #table_service #scripting #discovery #rate_limiter #locking #kesus
```

# Управление профилями

Профиль — именованный набор параметров соединения с БД, сохраненный в конфигурационном файле на локальной файловой системе. Использование профилей позволяет переиспользовать данные о размещении БД и параметрах аутентификации, сделав вызовы CLI существенно короче:

- Вызов команды `scheme ls` без профиля:
  ``` bash
  {{ ydb-cli }} \
  -e grpsc://some.host.in.some.domain:2136 \
  -d /some_long_identifier1/some_long_identifier2/database_name \
  --yc-token-file ~/secrets/token_database1 \
  scheme ls
  ```

- Вызов той же команды `scheme ls` с использованием профиля:
  ``` bash
  {{ ydb-cli }} -p quickstart scheme ls
  ```

## Команды управления профилями {#commands}

- [Создание профиля](../create.md)
- [Использование профиля](../use.md)
- [Получение списка профилей и параметров профиля](../list-and-get.md)
- [Удаление профиля](../delete.md)
- [Активация профиля и применение активированного профиля](../activate.md)

## Где хранятся профили {#location}

Профили хранятся локально в файле `~/ydb/config/config.yaml`.

{% include [location_overlay.md](location_overlay.md) %}






# Список объектов

Команда `scheme ls` позволяет получить список объектов в базе данных:

```bash
{{ ydb-cli }} [connection options] scheme ls [path] [-lR1]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

При запуске без параметров выводится перечень имен объектов в корневой директории базы данных в сжатом формате.

Параметром `path` можно задать [директорию](../dir.md), для которой нужно вывести перечень объектов.

Для команды доступны следующие опции:

* `-l` — полная информация об атрибутах каждого объекта;
* `-R` — рекурсивный обход всех поддиректорий;
* `-1` — выводить по одному объекту схемы на строку (например, для последующей обработки в скрипте).

**Примеры**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

- Получение объектов в корневой директории базы данных в сжатом формате

```bash
{{ ydb-cli }} --profile quickstart scheme ls
```

- Получение объектов во всех директориях базы данных в сжатом формате

```bash
{{ ydb-cli }} --profile quickstart scheme ls -R
```

- Получение объектов в заданной директории базы данных в сжатом формате

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1
{{ ydb-cli }} --profile quickstart scheme ls dir1/dir2
```

- Получение объектов во всех поддиректориях заданной директории базы данных в сжатом формате

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -R
{{ ydb-cli }} --profile quickstart scheme ls dir1/dir2 -R
```

- Получение полной информации по объектам в корневой директории базы данных

```bash
{{ ydb-cli }} --profile quickstart scheme ls -l
```

- Получение полной информации по объектам в заданной директории базы данных

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -l
{{ ydb-cli }} --profile quickstart scheme ls dir2/dir3 -l
```

- Получение полной информации по объектам во всех директориях базы данных

```bash
{{ ydb-cli }} --profile quickstart scheme ls -lR
```

- Получение полной информации по объектам во всех поддиректориях заданной директории базы данных

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -lR
{{ ydb-cli }} --profile quickstart scheme ls dir2/dir3 -lR
```


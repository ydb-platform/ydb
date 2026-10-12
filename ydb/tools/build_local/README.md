# Локальная сборка YDB в Docker

## TL;DR: Ubuntu и Bash

Нужны клон YDB, Bash и [Docker Engine](https://docs.docker.com/engine/install/ubuntu/).
Команда `docker info` должна работать от вашего пользователя;
настройка доступа описана в [инструкции Docker](https://docs.docker.com/engine/install/linux-postinstall/).
Все команды ниже выполняются из корня репозитория в Bash.

```bash
set -o pipefail
ydb/tools/build_local/run.sh --check
ydb/tools/build_local/run.sh ydb/apps/ydbd \
    2>&1 | tee .ydb-build/build.log | tail -100
```

Скрипт собирает образ Ubuntu 22.04 и запускает `ya make --build relwithdebinfo`.
По умолчанию контейнер получает 4 CPU и 24 GiB RAM без swap, бюджет `ya` — 20 GiB.
Компилятор скачивает `ya`. Бинарник сервера появится в
`.ydb-build/linux-amd64/ydb/apps/ydbd/ydbd`.

Статус сборки можно смотреть из другого терминала:

```bash
tail -f .ydb-build/build.log
```

## Тесты

Аргументы передаются в `ya make`. Запуск тестов включает сборку зависимостей:

```bash
set -o pipefail
ydb/tools/build_local/run.sh -tA ydb/core/kqp/ut/olap \
    -F 'KqpOlapLocks::*' 2>&1 | tail -100
```

Для одного C++ теста используйте `-F 'Suite::Test'`. Чтобы повторить уже пройденные
тесты, добавьте `--retest`: кеш компиляции при этом сохраняется.
Подробнее о фильтрах — в [TESTS.md](../../agents/TESTS.md).

Для тестов совместимости с текущими исходниками укажите `current`:

```bash
set -o pipefail
ydb/tools/build_local/run.sh -tA ydb/tests/compatibility/olap \
    -DYDB_COMPAT_TARGET_REF=current \
    -F 'test_compression.py::*' 2>&1 | tail -100
```

Остальные версии выбираются в
[versions.inc](../../tests/library/compatibility/versions.inc).
Без `current` тесты могут использовать только выпущенные версии YDB.

## Ограничения ресурсов

Лимиты задаются для каждого запуска:

```bash
YDB_BUILD_CPUS=6 YDB_BUILD_MEMORY=32g YA_MEMORY_LIMIT=28G \
    ydb/tools/build_local/run.sh ydb/apps/ydbd
```

`YDB_BUILD_MEMORY` — жёсткий лимит контейнера, `YA_MEMORY_LIMIT` — бюджет
планировщика сборки. Оставляйте между ними запас для линковщика и тестов.
Ограничения Docker действуют на контейнер сборки, а не на создание образа.

| Переменная | По умолчанию |
| --- | --- |
| `YDB_BUILD_CPUS` | `4` |
| `YDB_BUILD_MEMORY` | `24g` |
| `YA_MEMORY_LIMIT` | `20G` |
| `YDB_BUILD_TYPE` | `relwithdebinfo` |
| `YDB_BUILD_PLATFORM` | `linux/amd64` |
| `YDB_BUILD_OUTPUT` | `.ydb-build/<platform>` в репозитории |
| `YDB_BUILD_CACHE` | `ydb-ya-cache-<platform>-<uid>-<gid>` |
| `YDB_BUILD_UID`, `YDB_BUILD_GID` | UID/GID пользователя на Linux, `0` на macOS |
| `DOCKER_CONTEXT` | Текущий контекст Docker |

На Linux UID/GID позволяют записывать результаты от имени пользователя хоста.
На macOS используется root внутри VM для доступа к каталогам, смонтированным Colima.

## Кеш и место на диске

Повторяйте ту же команду после правок: `ya` переиспользует скачанные инструменты
и результаты сборки. Изменение `.cpp` обычно требует его пересборки и перелинковки,
изменение общего заголовка может затронуть много файлов. Лимиты CPU и памяти
не влияют на ключи кеша. Не меняйте исходники во время сборки.

Исходники монтируются только для чтения в `/src`, результаты — в `/out`.
Кеш `ya` хранится в отдельном Docker volume и остаётся после остановки контейнера.
Для его повторного использования нужны тот же Docker context, платформа и UID/GID
либо явно заданный `YDB_BUILD_CACHE`. Слои образа также кешируются: исходники YDB
в образ не копируются.

Сборка сервера и нескольких тестовых бинарников с отладочной информацией требует
сотен гигабайт. В VM закладывайте от 300 GiB под Docker и дополнительное место
на хосте под результаты; 150 GiB для такого набора оказалось недостаточно.
`run.sh --check` показывает свободное место, inode и действующие лимиты cgroup v2.

Для работы внутри контейнера:

```bash
ydb/tools/build_local/run.sh --shell
```

Внутри запускайте `/usr/local/bin/ydb-build <цель>`, чтобы сохранить параметры сборки.

## macOS / Apple Silicon

Для Colima из корня репозитория:

```bash
colima start ydb-build --activate=false --cpu 4 --memory 26 --disk 300 \
    --vm-type vz --vz-rosetta --mount "$PWD:w" --ssh-config=false
export DOCKER_CONTEXT=colima-ydb-build
ydb/tools/build_local/run.sh --check
```

Это отдельный профиль VM с запасом памяти сверх лимита контейнера.
`colima stop ydb-build` останавливает VM, сохраняя кеш.
В Docker Desktop задайте ресурсы VM в настройках.

По умолчанию на Apple Silicon собираются Linux x86-64 бинарники через Rosetta.
Это медленнее нативной сборки. `YDB_BUILD_PLATFORM=linux/arm64` выбирает отдельный
образ и кеш, но для ARM64 не все цели имеют необходимые ресурсы `ya`.

Если диск Colima заполнился, остановите сборку и увеличьте существующий диск:

```bash
colima stop ydb-build
colima start ydb-build --activate=false --ssh-config=false --disk 400
ydb/tools/build_local/run.sh --check
```

Выберите размер больше текущего и проверьте свободное место на хосте.
Подробнее — в [FAQ Colima](https://colima.run/docs/faq/).

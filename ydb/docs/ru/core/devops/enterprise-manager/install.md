# Скачивание и подготовка пакета {#install}

## Скачивание пакета {#download}

Скачайте актуальную версию пакета {{ ydb-short-name }} Enterprise Manager:

| Версия | Ссылка |
|--------|--------|
| 1.0.1  | [ydb-em-1.0.1-stable-linux-amd64.tar.xz](http://binaries.ydbem.website.yandexcloud.net/builds/1.0.1/ydb-em-1.0.1-stable-linux-amd64.tar.xz) |

<!-- TODO: Уточнить актуальный URL для скачивания и механизм обновления списка версий -->

## Состав пакета {#package-contents}

После распаковки архива вы получите следующие файлы:

| Файл | Описание |
|------|----------|
| `ydb-em-gateway` | Исполняемый файл {{ ydb-short-name }} EM Gateway |
| `ydb-em-cp` | Исполняемый файл {{ ydb-short-name }} EM Control Plane |
| `ydb-em-agent` | Исполняемый файл {{ ydb-short-name }} EM Agent |
| `ydb_platform-ydb-XXX.tar.gz` | Ansible-коллекция для {{ ydb-short-name }} |
| `ydb_platform-ydb_em-YYY.tar.gz` | Ansible-коллекция для {{ ydb-short-name }} EM |
| `examples.tar.gz` | Примеры конфигурации Ansible |
| `prepare.sh` | Скрипт автоматизации подготовительных шагов |

<!-- TODO: Уточнить, что конкретно делает prepare.sh и какие шаги он автоматизирует -->
<!-- TODO: Уточнить номера версий Ansible-коллекций (XXX, YYY) -->

## Установка Ansible-коллекций {#install-collections}

1. Установите Ansible-коллекцию для {{ ydb-short-name }}, если она ещё не установлена:

   ```bash
   ansible-galaxy collection install ydb_platform-ydb-1.2.0.tar.gz
   ```

2. Установите Ansible-коллекцию для {{ ydb-short-name }} Enterprise Manager:

   ```bash
   ansible-galaxy collection install ydb_platform-ydb_em-1.0.1.tar.gz
   ```

## Подготовка рабочей директории {#prepare-workspace}

1. Распакуйте файл `examples.tar.gz`:

   ```bash
   tar -xzf examples.tar.gz
   ```

2. Переместите все исполняемые файлы из пакета в директорию `examples/files`:

   ```bash
   cp ydb-em-gateway ydb-em-cp ydb-em-agent examples/files/
   ```

3. Поместите TLS-сертификаты в директорию `examples/files/certs`.

<!-- TODO: Уточнить формат и требования к TLS-сертификатам (CA, серверные, клиентские) -->
<!-- TODO: Уточнить структуру директории examples после распаковки -->

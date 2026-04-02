# Nemesis App

Приложение для инъекции неисправностей (fault injection) в YDB кластер.

## Режимы работы

- **master** (orchestrator) — управляет агентами, предоставляет UI
- **agent** — выполняет команды на хосте

## Конфигурация

Переменные окружения (или `.env` файл):

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `NEMESIS_TYPE` | Режим: `master` или `agent` | `master` |
| `APP_HOST` | Адрес хоста для привязки | `::` |
| `APP_PORT` | Порт приложения | `31434` |
| `STATIC_LOCATION` | Путь к статическим файлам | `static` |
| `YAML_CONFIG_LOCATION` | Путь к cluster.yaml | — |

## Установка на кластер и запуск

```bash
# Из директории с бинарником nemesis
./nemesis --yaml-config-location /your/path/to/config.yaml install
```

Первый хост из `cluster.yaml` становится orchestrator, остальные — агентами.

Порт по умолчанию: **31434** (настраивается через `APP_PORT`)

## Остановка сервисов

```bash
./nemesis stop
```

## UI

После запуска: `http://<orchestrator_host>:31434/static/index.html`
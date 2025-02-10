# Миграция на динамическую конфигурацию

Данный документ содержит инструкцию по миграции со [статической конфигурации](../before-v25.1/configuration-management/config-overview.md#static-config) кластера {{ydb-short-name}} на [динамическую](../before-v25.1/configuration-management/config-overview.md#dynamic-config).

## Исходное состояние

Миграция на динамическую конфигурацию может быть осуществлена в случае выполнения следующих условий:

1. Кластер {{ydb-short-name}} [обновлен](../../../maintenance/upgrade.md) до версии 25.1 и выше.
1. Кластер {{ydb-short-name}} сконфигурирован с файлом [статической конфигурации](../before-v25.1/configuration-management/config-overview.md#static-config) `config.yaml`, разложенным по узлам и подключенным через аргумент `ydbd --yaml-config`.
1. В систему загружен файл [динамической конфигурации](../before-v25.1/configuration-management/config-overview.md#dynamic-config) `dynconfig.yaml`.

## Переход на динамическую конфигурацию

Для того чтобы перевести кластер {{ ydb-short-name }} на механизм динамической конфигурации, необходимо проделать следующие шаги:

1. Получить текущую динамическую конфигурацию кластера:

```bash
ydb -e grpc://<node.ydb.tech>:2135 admin config fetch > config.yaml
```

2. Разложить файл `config.yaml` по всем нодам кластера, заменив старый файл конфигурации.
3. Выполнить следующую команду с новым конфигурационным файлом:

```bash
ydb -e grpc://<node.ydb.tech>:2135 admin storage replace -f config.yaml
```

4. Перезапустить все узлы кластера с помощью процедуры [rolling-restart](../../../../maintenance/manual/node_restarting.md).

В результате проделанных действий кластер будет переведён в режим работы с файлом динамической конфигурации. Файл статической конфигурации больше не используется.

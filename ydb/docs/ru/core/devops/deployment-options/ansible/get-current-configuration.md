# Получение текущей конфигурации кластера

## Требования

- предустановленный кластер с любой конфигурацией

## Шаги

1. Запустите плейбук `generate_conf` для получения текущей конфигурации кластера:

   ```bash
   ansible-playbook ydb_platform.ydb.generate_conf
   ```

2. Файлы конфигурации будут сохранены в директории `ydb_cluster_config`:

   - Для кластеров V1:
     - `ydb_cluster_config/ydbd-config-static.yaml` — статическая конфигурация
     - `ydb_cluster_config/ydb_cluster_dynconfig.yaml` — динамическая конфигурация
   - Для кластеров V2:
     - `ydb_cluster_config.yaml` — единый файл конфигурации
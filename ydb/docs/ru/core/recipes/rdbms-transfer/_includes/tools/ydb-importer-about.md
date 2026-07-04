## ydb-importer {#about}

**ydb-importer** — Java-утилита для параллельного импорта из JDBC-источников в {{ ydb-short-name }} через Bulk Upsert. Конфигурация — XML; поддерживаются PostgreSQL, MySQL, Oracle, MSSQL, Db2 и другие JDBC-СУБД.

### Системные требования

| Компонент | Требование |
| --- | --- |
| JDK | **8+** (OpenJDK 8+); команда `java` в `PATH` |
| RAM | Зависит от `pool size` и размера batch; **≥ 4 ГБ** для типичного импорта |
| Диск | Распакованный дистрибутив + JDBC JAR источника в `lib/` |
| Сеть | Доступ к JDBC-источнику и {{ ydb-short-name }} с хоста, где запускается утилита |

### Установка

1. Скачайте архив с [страницы релизов](https://github.com/ydb-platform/ydb-importer/releases).
2. Распакуйте, положите JDBC-драйвер источника в `lib/*.jar`.
3. Запуск: `./ydb-importer.sh my-config.xml`.

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md).

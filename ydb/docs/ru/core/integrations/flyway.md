# Миграции схемы данных {{ ydb-short-name }} с помощью Flyway

## Введение {#introduction}

[Flyway](https://documentation.red-gate.com/fd/) - это инструмент для миграции баз данных с открытым исходным кодом. Она имеет расширения для различных систем управления базами данных (СУБД), включая {{ ydb-short-name }}.

## Установка {#install}

Чтобы использовать flyway вместе с YDB в Java / Kotlin приложении или в Gradle / Maven плагине требуется зависимость расширения flyway для YDB и [YDB JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver):

{% list tabs %}

- Maven

  ```xml
  <!-- Set an actual versions -->
  <dependency>
      <groupId>org.flywaydb</groupId>
      <artifactId>flyway-core</artifactId>
      <version>${flyway.core.version}</version>
  </dependency>
  
  <dependency>
      <groupId>tech.ydb.jdbc</groupId>
      <artifactId>ydb-jdbc-driver</artifactId>
      <version>${ydb.jdbc.version}</version>
  </dependency>
  
  <dependency>
      <groupId>tech.ydb.dialects</groupId>
      <artifactId>flyway-ydb-dialect</artifactId>
      <version>${flyway.ydb.dialect.version}</version>
  </dependency>
  ```

- Gradle

  ```groovy
  dependencies {
      // Set an actual versions
      implementation "org.flywaydb:flyway-core:$flywayCoreVersion"
      implementation "tech.ydb.dialects:flyway-ydb-dialect:$flywayYdbDialecVersion"
      implementation "tech.ydb.jdbc:ydb-jdbc-driver:$ydbJdbcVersion"
  }
  ```

{% endlist %}

Для работы с YDB через flyway CLI требуется установить саму flyway утилиту [любым из рекомендованных способов](https://documentation.red-gate.com/fd/command-line-184127404.html). 

Затем утилиту нужно расширить диалектом YDB и JDBC драйвером.

```bash
# install flyway
# cd $(which flyway) // prepare this command for your environment

cd libexec
# set an actual versions .jar files

cd drivers && curl -L -o ydb-jdbc-driver-shaded-2.1.0.jar https://repo.maven.apache.org/maven2/tech/ydb/jdbc/ydb-jdbc-driver-shaded/2.1.0/ydb-jdbc-driver-shaded-2.1.0.jar

cd ..

cd lib && curl -L -o flyway-ydb-dialect.jar https://repo.maven.apache.org/maven2/tech/ydb/dialects/flyway-ydb-dialect/1.0.0-RC0/flyway-ydb-dialect-1.0.0-RC0.jar
```

## Управление миграциями с помощью flyway {#flyway-main-commands}

### baseline {#flyway-baseline}

Команда [baseline](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-baseline) инициализируют flyway в существующей базе данных, исключающая все миграции вплоть до baselineVersion включительно.

Предположим, что мы имеем существующий проект с текущей схемой базы данных:

![_assets/flyway-baseline-step-1.png](_assets/flyway-baseline-step-1.png)

Запишем наши существующие миграции следующим образом:

```
db/migration:
  V1__create_series.sql
  V2__create_seasons.sql
  V3__create_episodes.sql
```

Установим `baselineVersion = 3`, затем выполним следующую команду:

```bash
flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration -baselineVersion=3 baseline
```

Результатом исполнения будет созданная таблица `flyway_schema_history` с `baseline` записью:

![_assets/flyway-baseline-step-2.png](_assets/flyway-baseline-step-2.png)

### migrate {#flyway-migrate}

Команда [migrate](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-migrate) эволюционирует схему базы данных до последней версии, если таблица истории схемы не была создана, то создаст ее автоматически.

Добавим к предыдущему примеру миграцию [загрузки данных](../yql/reference/tutorial/fill_tables_with_data.md):

```
db/migration:
  V1__create_series.sql
  V2__create_seasons.sql
  V3__create_episodes.sql
  V4__load_data.sql
```

Применим последнюю миграцию следующей командой:

```bash
flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration migrate
```

Результатом исполнения будет загруженные данные в таблицы `series`, `seasons` и `episodes`:

![_assets/flyway-migrate-step-1.png](_assets/flyway-migrate-step-1.png)

[Загрузим данные](../yql/reference/tutorial/fill_tables_with_data.md) без команды `COMMIT` в конце скрипта, [создадим индекс](../yql/reference/syntax/alter_table.md), затем переименуем индекс. 

Эволюционируем схему путем добавление [вторичного индекса](../yql/reference/syntax/alter_table.md):

```
db/migration:
  V1__create_series.sql
  V2__create_seasons.sql
  V3__create_episodes.sql
  V4__load_data.sql
  V5__create_series_title_index.sql
```

Применим последнюю миграцию следующей командой:

```bash
flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration migrate
```

Результатом будет появление вторичного индекса у таблицы `series`:

![_assets/flyway-migrate-step-2.png](_assets/flyway-migrate-step-2.png)

### info {#flyway-info}

Команда [info](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-info) печатает подробные сведения и информацию о состоянии всех миграций.

Добавим еще одну миграцию, которая переименовывает раннее добавленный вторичный индекс: 

```
db/migration:
  V1__create_series.sql
  V2__create_seasons.sql
  V3__create_episodes.sql
  V4__load_data.sql
  V5__create_series_title_index.sql
  V6__rename_index_title_index.sql
```

Результатом исполнения команды `flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration info` будет подробная информация о состоянии миграций:

```
+-----------+---------+---------------------------+----------+---------------------+--------------------+----------+
| Category  | Version | Description               | Type     | Installed On        | State              | Undoable |
+-----------+---------+---------------------------+----------+---------------------+--------------------+----------+
| Versioned | 1       | create series             | SQL      |                     | Below Baseline     | No       |
| Versioned | 2       | create seasons            | SQL      |                     | Below Baseline     | No       |
| Versioned | 3       | create episodes           | SQL      |                     | Ignored (Baseline) | No       |
|           | 3       | << Flyway Baseline >>     | BASELINE | 2024-04-16 12:09:27 | Baseline           | No       |
| Versioned | 4       | load data                 | SQL      | 2024-04-16 12:35:12 | Success            | No       |
| Versioned | 5       | create series title index | SQL      | 2024-04-16 12:59:20 | Success            | No       |
| Versioned | 6       | rename index title index  | SQL      |                     | Pending            | No       |
+-----------+---------+---------------------------+----------+---------------------+--------------------+----------+
```

### validate {#flyway-validate}

Команда [validate](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-validate) проверяет соответствие примененных миграций к миграциям, которые находятся в файловой системе пользователя.

После применения к текущим миграциям команды `flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration validate`, в логах будет написано, что последняя миграция не была применена к нашей базе данных:

```bash
ERROR: Validate failed: Migrations have failed validation
Detected resolved migration not applied to database: 6.
To fix this error, either run migrate, or set -ignoreMigrationPatterns='*:pending'.
```

Давайте ее применим, выполнив `flyway .. migrate`. Теперь валидация проходит успешно, вторичный индекс переименован.

Далее изменим файл уже ранее примененной миграции `V4__load_date.sql`, удалив комментарии в SQL скрипте.

После исполнения команды валидации, получим закономерную ошибку о том, что `checksum` различается в измененной миграции:

```bash
ERROR: Validate failed: Migrations have failed validation
Migration checksum mismatch for migration version 4
-> Applied to database : 591649768
-> Resolved locally    : 1923849782
```

### repair {#flyway-repair}

Команда [repair](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-repair) пытается устранить выявленные ошибки и расхождения с таблицей историей схемы базы данных.

Устраним проблему с разными `checksum`, выполнив следующую команду:

```bash
flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration repair
```

Результатом будет обновление колонки `checksum` в таблице `flyway_schema_history` у записи, отвечающей за миграцию `V4__load_data.sql`:

![_assets/flyway-repair-step-1.png](_assets/flyway-repair-step-1.png)

После восстановления таблицы лога, валидация проходит успешно. 

C помощью команды `repair` можно удалить не удавшийся DDL скрипт.

### clean {#flyway-repair}

Команда [clean](https://documentation.red-gate.com/flyway/flyway-cli-and-api/usage/command-line/command-line-clean) удаляет все таблицы в схеме базы данных.

{% note warning %}

YDB не имеет такую сущность как `schema`, команда `clean` просто удалит все таблицы в вашей базе данных.

{% endnote %}

Удалим все таблицы в нашей базе данных следующей командой:

```bash
flyway -url=jdbc:ydb:grpc://localhost:2136/local -locations=db/migration -cleanDisabled=false clean
```

Результатом будет пустая база данных:

![_assets/flyway-clean-step-1.png](_assets/flyway-clean-step-1.png)

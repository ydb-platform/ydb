# Загрузка примеров данных в {{ ydb-short-name }}

На этой странице описаны популярные наборы данных, которые вы можете загрузить в {{ ydb-short-name }} для ознакомления с функциональностью базы данных и тестирования различных сценариев использования.

## Предварительные требования

Для загрузки датасетов вам потребуется:

1. Установленный [{{ ydb-short-name }} CLI](../../reference/ydb-cli/)
2. [Опционально] Настроенный [профиль подключения](../../reference/ydb-cli/profile/create.md) к {{ ydb-short-name }}

## Общая информация о загрузке данных

{{ ydb-short-name }} поддерживает импорт данных из CSV-файлов с помощью [команды](../../reference/ydb-cli/export-import/import-file.md) `ydb import file csv`. Пример запуска команды:

```bash
ydb import file csv --header --null-value "" --path <путь_к_таблице> <файл>.csv
```

Опция `--header` означает, что в первой строчке файла содержится список имён колонок, а сами данные начинаются со второй строчки.

Опцией `--null-value` можно задать строку, которая будет восприниматься как null-значение при импорте.

Для импорта данных нужна заранее созданная таблица в {{ ydb-short-name }}. Основной способ создания таблицы - выполнить YQL-запрос `CREATE TABLE`. Чтобы не составлять его полностью вручную, можно попробовать выполнить команду импорта из файла. В случае отстутствия в базе таблицы, CLI предложит текст `CREATE TABLE`, который можно будет взять за основу и при необходимости отредактировать.

В {{ ydb-short-name }} критически важно, чтобы таблица имела первичный ключ. Он существенно влияет на скорость загрузки и обработки данных, а также служит для дедупликации. Строки с идентичными значениями в колонках первичного ключа заменяют друг друга.

## Популярные наборы данных

### E-Commerce Behavior Data

Данные о поведении пользователей в мультикатегорийном интернет-магазине.

**Источник**: [Kaggle - E-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data)

**Размер**: 8.7 GB

**Пример загрузки**:

1. Скачайте файл `2019-Nov.csv` с Kaggle
2. Создайте таблицу в {{ ydb-short-name }}

<details>
  <summary>Выполнив запрос в [WEB-интерфейсе](../../reference/embedded-ui/ydb-monitoring)</summary>

  ```sql
  CREATE TABLE `ecommerce_table` (
      `event_time` Text NOT NULL,  -- Формат: "2019-11-01 00:00:00 UTC"
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`event_time`, `product_id`, `user_id`)
  );
  ```
</details>

<details>
  <summary>Выполнив команду через CLI</summary>

  ```bash
  ydb sql -s \
  'CREATE TABLE `ecommerce_table` (
      `event_time` Text NOT NULL,  -- Формат: "2019-11-01 00:00:00 UTC"
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`event_time`, `product_id`, `user_id`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );'
  ```
</details>

**Примечание**: Поле `event_time` имеет тип Text, так как значения в формате "2019-11-01 00:00:00 UTC" не могут быть автоматически преобразованы в тип DateTime.

3. Выполните команду импорта

```bash
ydb import file csv --header --null-value "" --path ecommerce_table 2019-Nov.csv
```

### Animal Crossing New Horizons Catalog

Каталог предметов из популярной игры Animal Crossing: New Horizons.

**Источник**: [Kaggle - Animal Crossing New Horizons Catalog](https://www.kaggle.com/datasets/jessicali9530/animal-crossing-new-horizons-nookplaza-dataset/)

**Размер**: 3.63 MB (30 файлов)

**Пример загрузки**:

1. Скачайте файл `accessories.csv` с Kaggle
2. Удалите BOM-байты из начала файла. Например, выполнив команду:

```bash
sed -i '1s/^\xEF\xBB\xBF//' accessories.csv
```

2. Уберите пробелы из имён колонок. Например, выполнив команду:

```bash
sed -i '1s/ /_/g' accessories.csv
```

4. Создайте таблицу в {{ ydb-short-name }}
<details>
  <summary>Выполнив запрос в [WEB-интерфейсе](../../reference/embedded-ui/ydb-monitoring)</summary>

  ```sql
  CREATE TABLE `accessories` (
      `Name` Text NOT NULL,
      `Variation` Text NOT NULL,
      `DIY` Text NOT NULL,
      `Buy` Text NOT NULL,
      `Sell` Uint64 NOT NULL,
      `Color_1` Text NOT NULL,
      `Color_2` Text NOT NULL,
      `Size` Text NOT NULL,
      `Miles_Price` Text NOT NULL,
      `Source` Text NOT NULL,
      `Source_Notes` Text NOT NULL,
      `Seasonal_Availability` Text NOT NULL,
      `Mannequin_Piece` Text NOT NULL,
      `Version` Text NOT NULL,
      `Style` Text NOT NULL,
      `Label_Themes` Text NOT NULL,
      `Type` Text NOT NULL,
      `Villager_Equippable` Text NOT NULL,
      `Catalog` Text NOT NULL,
      `Filename` Text NOT NULL,
      `Internal_ID` Uint64 NOT NULL,
      `Unique_Entry_ID` Text NOT NULL,
      PRIMARY KEY (`Unique_Entry_ID`)
  )
  WITH (
      STORE = COLUMN
  );
  ```
</details>

<details>
  <summary>* Выполнив команду через CLI</summary>

  ```bash
  ydb sql -s \
  'CREATE TABLE `accessories` (
      `Name` Text NOT NULL,
      `Variation` Text NOT NULL,
      `DIY` Text NOT NULL,
      `Buy` Text NOT NULL,
      `Sell` Uint64 NOT NULL,
      `Color_1` Text NOT NULL,
      `Color_2` Text NOT NULL,
      `Size` Text NOT NULL,
      `Miles_Price` Text NOT NULL,
      `Source` Text NOT NULL,
      `Source_Notes` Text NOT NULL,
      `Seasonal_Availability` Text NOT NULL,
      `Mannequin_Piece` Text NOT NULL,
      `Version` Text NOT NULL,
      `Style` Text NOT NULL,
      `Label_Themes` Text NOT NULL,
      `Type` Text NOT NULL,
      `Villager_Equippable` Text NOT NULL,
      `Catalog` Text NOT NULL,
      `Filename` Text NOT NULL,
      `Internal_ID` Uint64 NOT NULL,
      `Unique_Entry_ID` Text NOT NULL,
      PRIMARY KEY (`Unique_Entry_ID`)
  )
  WITH (
      STORE = COLUMN
  );'
  ```
</details>

3. Выполните команду импорта

```bash
ydb import file csv --header --path accessories accessories.csv
```

## Особенности и ограничения

При работе с загрузкой CSV-файлов в {{ ydb-short-name }} следует учитывать следующие моменты:

1. **Имена колонок**: Названия колонок не должны содержать пробелы или специальные символы.

2. **Типы данных**:
   - Строки в формате даты/времени с указанием временной зоны (например, "2019-11-01 00:00:00 UTC") будут импортированы как тип Text
   - Тип Bool не поддерживается в качестве типа колонки, используйте Text или Int64

3. **Производительность**: Для больших файлов рекомендуется настраивать параметры `--batch-bytes` и `--max-in-flight` для оптимизации процесса импорта.

## Часто задаваемые вопросы

### Как обрабатывать NULL-значения?

Используйте параметр `--null-value` для указания строки, которая должна интерпретироваться как NULL. Например: `--null-value ""`.

### Как пропустить заголовок в CSV-файле?

Используйте параметр `--header` для пропуска строки заголовка или `--skip-rows N` для пропуска N строк в начале файла.

### Как выбрать колонки для импорта?

Используйте параметр `--columns` для указания списка имён колонок в файле.

## Дополнительные ресурсы

- [Документация {{ ydb-short-name }} CLI](https://ydb.tech/ru/docs/reference/ydb-cli/commands/import)
- [Руководство по работе с {{ ydb-short-name }} SQL](https://ydb.tech/ru/docs/yql/reference/)
- [GitHub-репозиторий {{ ydb-short-name }}](https://github.com/ydb-platform/ydb)
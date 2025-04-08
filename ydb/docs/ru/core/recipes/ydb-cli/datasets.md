# Импорт датасетов в {{ ydb-short-name }}

На этой странице описаны популярные датасеты, которые вы можете загрузить в {{ ydb-short-name }} для ознакомления с функциональностью базы данных и тестирования различных сценариев использования.

## Предварительные требования

Для загрузки датасетов вам потребуется:

1. Установленный [{{ ydb-short-name }} CLI](../../reference/ydb-cli/)
2. [Опционально] Настроенный [профиль подключения](../../reference/ydb-cli/profile/create.md) к {{ ydb-short-name }}, чтобы не указывать параметры подключения при каждом вызове

## Общая информация о загрузке данных

{{ ydb-short-name }} поддерживает импорт данных из CSV-файлов с помощью [команды](../../reference/ydb-cli/export-import/import-file.md) `ydb import file csv`. Пример запуска команды:

```bash
ydb import file csv --header --null-value "" --path <путь_к_таблице> <файл>.csv
```

Где:
* `--header` означает, что в первой строке файла содержится список имён колонок, а сами данные начинаются со второй строки;
* `--null-value` задает строку, которая будет восприниматься как null-значение при импорте.

Для импорта данных нужна заранее созданная таблица в {{ ydb-short-name }}. Основной способ создания таблицы - выполнить [YQL-запрос `CREATE TABLE`](../../../core/yql/reference/syntax/create_table/index.md). Чтобы не составлять его полностью вручную, можно попробовать выполнить команду импорта из файла, как в любом примере ниже, не создавая перед этим таблицу. В таком случае CLI предложит текст `CREATE TABLE`, который можно будет взять за основу, при необходимости отредактировать и выполнить.

{% note info "Выбор первичного ключа" %}

В {{ ydb-short-name }} критически важно, чтобы таблица имела первичный ключ. Он существенно влияет на скорость загрузки и обработки данных, а также служит для дедупликации. Строки с идентичными значениями в колонках первичного ключа заменяют друг друга.

{% endnote %}

## Особенности и ограничения

При работе с загрузкой CSV-файлов в {{ ydb-short-name }} следует учитывать следующие моменты:

1. **Имена колонок**: Названия колонок не должны содержать пробелы или специальные символы.

2. **Типы данных**:
   - Строки в формате даты/времени с указанием временной зоны (например, "2019-11-01 00:00:00 UTC") будут импортированы как тип Text
   - Тип Bool не поддерживается в качестве типа колонки, используйте Text или Int64

## Примеры импорта датасетов

### Video Game Sales

Данные о продажах видеоигр.

**Источник**: [Kaggle - Video Game Sales](https://www.kaggle.com/datasets/gregorut/videogamesales)

**Размер**: 1.36 MB

**Пример загрузки**:

1. Скачайте и разархивируйте файл `vgsales.csv` с Kaggle

2. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `vgsales` (
      `Rank` Uint64 NOT NULL,
      `Name` Text NOT NULL,
      `Platform` Text NOT NULL,
      `Year` Text NOT NULL,
      `Genre` Text NOT NULL,
      `Publisher` Text NOT NULL,
      `NA_Sales` Double NOT NULL,
      `EU_Sales` Double NOT NULL,
      `JP_Sales` Double NOT NULL,
      `Other_Sales` Double NOT NULL,
      `Global_Sales` Double NOT NULL,
      PRIMARY KEY (`Rank`)
  )
  WITH (
      STORE = COLUMN
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `vgsales` (
      `Rank` Uint64 NOT NULL,
      `Name` Text NOT NULL,
      `Platform` Text NOT NULL,
      `Year` Text NOT NULL,
      `Genre` Text NOT NULL,
      `Publisher` Text NOT NULL,
      `NA_Sales` Double NOT NULL,
      `EU_Sales` Double NOT NULL,
      `JP_Sales` Double NOT NULL,
      `Other_Sales` Double NOT NULL,
      `Global_Sales` Double NOT NULL,
      PRIMARY KEY (`Rank`)
  )
  WITH (
      STORE = COLUMN
  );'
  ```

{% endlist %}

3. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path vgsales vgsales.csv
```

4. Чтобы определить издателя, у которого наибольшая средняя продажа игр в Северной Америке, выполните запрос:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      Publisher, 
      AVG(NA_Sales) AS average_na_sales
  FROM vgsales
  GROUP BY Publisher
  ORDER BY average_na_sales DESC
  LIMIT 1;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      Publisher, 
      AVG(NA_Sales) AS average_na_sales
  FROM vgsales
  GROUP BY Publisher
  ORDER BY average_na_sales DESC
  LIMIT 1;'
  ```

{% endlist %}

Пример результата:

```
┌───────────┬──────────────────┐
│ Publisher │ average_na_sales │
├───────────┼──────────────────┤
│ "Palcom"  │ 3.38             │
└───────────┴──────────────────┘
```

Запрос позволит найти, какой издатель достиг наибольшего успеха в Северной Америке по средней продаже.

### E-Commerce Behavior Data

Данные о поведении пользователей в мультикатегорийном интернет-магазине.

**Источник**: [Kaggle - E-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data)

**Размер**: 9 GB

**Пример загрузки**:

1. Скачайте и разархивируйте файл `2019-Nov.csv` с Kaggle

2. Датасет включает в себя полностью идентичные строки. Поскольку YDB требует указания уникальных значений первичного ключа, добавим в файл новую колонку под названием `row_id`, где значение ключа будет равно номеру строки в исходном файле. Это позволит предотвратить удаление повторяющихся данных. Эту операцию можно осуществить с помощью команды awk:

```bash
awk 'NR==1 {print "row_id," $0; next} {print NR-1 "," $0}' 2019-Nov.csv > temp.csv && mv temp.csv 2019-Nov.csv
```

3. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `ecommerce_table` (
      `row_id` Uint64 NOT NULL,
      `event_time` Text NOT NULL,
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `ecommerce_table` (
      `row_id` Uint64 NOT NULL,
      `event_time` Text NOT NULL,
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );'
  ```
{% endlist %}

4. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path ecommerce_table 2019-Nov.csv
```

5. Выполните аналитический запрос для определения самых популярных категорий продуктов 1 ноября 2019 года:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      category_code, 
      COUNT(*) AS view_count
  FROM ecommerce_table
  WHERE 
      SUBSTRING(CAST(event_time AS String), 0, 10) = '2019-11-01' 
      AND event_type = 'view'
  GROUP BY category_code
  ORDER BY view_count DESC
  LIMIT 10;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      category_code, 
      COUNT(*) AS view_count
  FROM ecommerce_table
  WHERE 
      SUBSTRING(CAST(event_time AS String), 0, 10) = "2019-11-01" 
      AND event_type = "view"
  GROUP BY category_code
  ORDER BY view_count DESC
  LIMIT 10;'
  ```

{% endlist %}

Пример результата:

```
┌────────────────────────────────────┬────────────┐
│ category_code                      │ view_count │
├────────────────────────────────────┼────────────┤
│ null                               │ 453024     │
├────────────────────────────────────┼────────────┤
│ "electronics.smartphone"           │ 360650     │
├────────────────────────────────────┼────────────┤
│ "electronics.clocks"               │ 43581      │
├────────────────────────────────────┼────────────┤
│ "computers.notebook"               │ 40878      │
├────────────────────────────────────┼────────────┤
│ "electronics.video.tv"             │ 40383      │
├────────────────────────────────────┼────────────┤
│ "electronics.audio.headphone"      │ 37489      │
├────────────────────────────────────┼────────────┤
│ "apparel.shoes"                    │ 31013      │
├────────────────────────────────────┼────────────┤
│ "appliances.kitchen.washer"        │ 28028      │
├────────────────────────────────────┼────────────┤
│ "appliances.kitchen.refrigerators" │ 27808      │
├────────────────────────────────────┼────────────┤
│ "appliances.environment.vacuum"    │ 26477      │
└────────────────────────────────────┴────────────┘
```

### COVID-19 Open Research Dataset

Открытый набор данных исследований COVID-19.

**Источник**: [Kaggle - COVID-19 Open Research Dataset Challenge](https://www.kaggle.com/datasets/allen-institute-for-ai/CORD-19-research-challenge?select=metadata.csv)

**Размер**: 1.65 GB (файл metadata.csv)

**Пример загрузки**:

1. Скачайте и разархивируйте файл `metadata.csv` с Kaggle

2. Датасет включает в себя полностью идентичные строки. Поскольку YDB требует указания уникальных значений первичного ключа, добавим в файл новую колонку под названием `row_id`, где значение ключа будет равно номеру строки в исходном файле. Это позволит предотвратить удаление повторяющихся данных. Эту операцию можно осуществить с помощью команды awk:

```bash
awk 'NR==1 {print "row_id," $0; next} {print NR-1 "," $0}' metadata.csv > temp.csv && mv temp.csv metadata.csv
```

3. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `covid_research` (
      `row_id` Uint64 NOT NULL,
      `cord_uid` Text NOT NULL,
      `sha` Text NOT NULL,
      `source_x` Text NOT NULL,
      `title` Text NOT NULL,
      `doi` Text NOT NULL,
      `pmcid` Text NOT NULL,
      `pubmed_id` Text NOT NULL,
      `license` Text NOT NULL,
      `abstract` Text NOT NULL,
      `publish_time` Text NOT NULL,
      `authors` Text NOT NULL,
      `journal` Text NOT NULL,
      `mag_id` Text,
      `who_covidence_id` Text,
      `arxiv_id` Text,
      `pdf_json_files` Text NOT NULL,
      `pmc_json_files` Text NOT NULL,
      `url` Text NOT NULL,
      `s2_id` Uint64,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `covid_research` (
      `row_id` Uint64 NOT NULL,
      `cord_uid` Text NOT NULL,
      `sha` Text NOT NULL,
      `source_x` Text NOT NULL,
      `title` Text NOT NULL,
      `doi` Text NOT NULL,
      `pmcid` Text NOT NULL,
      `pubmed_id` Text NOT NULL,
      `license` Text NOT NULL,
      `abstract` Text NOT NULL,
      `publish_time` Text NOT NULL,
      `authors` Text NOT NULL,
      `journal` Text NOT NULL,
      `mag_id` Text,
      `who_covidence_id` Text,
      `arxiv_id` Text,
      `pdf_json_files` Text NOT NULL,
      `pmc_json_files` Text NOT NULL,
      `url` Text NOT NULL,
      `s2_id` Uint64,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN
  );'
  ```

{% endlist %}

4. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path covid_research metadata.csv
```

5. Выполните аналитический запрос для определения журналов с наибольшим количеством публикаций:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      journal,
      COUNT(*) AS publication_count
  FROM covid_research
  WHERE journal IS NOT NULL AND journal != ''
  GROUP BY journal
  ORDER BY publication_count DESC
  LIMIT 10;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      journal,
      COUNT(*) AS publication_count
  FROM covid_research
  WHERE journal IS NOT NULL AND journal != ""
  GROUP BY journal
  ORDER BY publication_count DESC
  LIMIT 10;'
  ```

{% endlist %}

Пример результата:

```
┌───────────────────────────────────┬───────────────────┐
│ journal                           │ publication_count │
├───────────────────────────────────┼───────────────────┤
│ "PLoS One"                        │ 9953              │
├───────────────────────────────────┼───────────────────┤
│ "bioRxiv"                         │ 8961              │
├───────────────────────────────────┼───────────────────┤
│ "Int J Environ Res Public Health" │ 8201              │
├───────────────────────────────────┼───────────────────┤
│ "BMJ"                             │ 6928              │
├───────────────────────────────────┼───────────────────┤
│ "Sci Rep"                         │ 5935              │
├───────────────────────────────────┼───────────────────┤
│ "Cureus"                          │ 4212              │
├───────────────────────────────────┼───────────────────┤
│ "Reactions Weekly"                │ 3891              │
├───────────────────────────────────┼───────────────────┤
│ "Front Psychol"                   │ 3541              │
├───────────────────────────────────┼───────────────────┤
│ "BMJ Open"                        │ 3515              │
├───────────────────────────────────┼───────────────────┤
│ "Front Immunol"                   │ 3442              │
└───────────────────────────────────┴───────────────────┘
```

### Netflix Movies and TV Shows

Данные о фильмах и сериалах на платформе Netflix.

**Источник**: [Kaggle - Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)

**Размер**: 3.4 MB

**Пример загрузки**:

1. Скачайте и разархивируйте файл `netflix_titles.csv` с Kaggle

2. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `netflix` (
      `show_id` Text NOT NULL,
      `type` Text NOT NULL,
      `title` Text NOT NULL,
      `director` Text NOT NULL,
      `cast` Text,
      `country` Text NOT NULL,
      `date_added` Text NOT NULL,
      `release_year` Uint64 NOT NULL,
      `rating` Text NOT NULL,
      `duration` Text NOT NULL,
      `listed_in` Text NOT NULL,
      `description` Text NOT NULL,
      PRIMARY KEY (`show_id`)
  )
  WITH (
      STORE = COLUMN
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `netflix` (
      `show_id` Text NOT NULL,
      `type` Text NOT NULL,
      `title` Text NOT NULL,
      `director` Text NOT NULL,
      `cast` Text,
      `country` Text NOT NULL,
      `date_added` Text NOT NULL,
      `release_year` Uint64 NOT NULL,
      `rating` Text NOT NULL,
      `duration` Text NOT NULL,
      `listed_in` Text NOT NULL,
      `description` Text NOT NULL,
      PRIMARY KEY (`show_id`)
  )
  WITH (
      STORE = COLUMN
  );'
  ```

{% endlist %}

3. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path netflix netflix_titles.csv
```

4. Выполните аналитический запрос, чтобы определить три страны, из которых было добавлено больше всего контента на Netflix в 2020 году:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      country, 
      COUNT(*) AS count
  FROM netflix
  WHERE 
      CAST(SUBSTRING(CAST(date_added AS String), 7, 4) AS Int32) = 2020
      AND date_added IS NOT NULL
  GROUP BY country
  ORDER BY count DESC
  LIMIT 3;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      country, 
      COUNT(*) AS count
  FROM netflix
  WHERE 
      CAST(SUBSTRING(CAST(date_added AS String), 7, 4) AS Int32) = 2020
      AND date_added IS NOT NULL
  GROUP BY country
  ORDER BY count DESC
  LIMIT 3;'
  ```

{% endlist %}

Пример результата:

```
┌─────────────────┬───────┐
│ country         │ count │
├─────────────────┼───────┤
│ "United States" │ 22    │
├─────────────────┼───────┤
│ ""              │ 7     │
├─────────────────┼───────┤
│ "Canada"        │ 3     │
└─────────────────┴───────┘
```

### Animal Crossing New Horizons Catalog

Каталог предметов из популярной игры Animal Crossing: New Horizons.

**Источник**: [Kaggle - Animal Crossing New Horizons Catalog](https://www.kaggle.com/datasets/jessicali9530/animal-crossing-new-horizons-nookplaza-dataset/)

**Размер**: 51 KB

**Пример загрузки**:

1. Скачайте и разархивируйте файл `accessories.csv` с Kaggle

2. Этот файл включает в себя маску BOM (Byte Order Mark). Однако команда импорта не поддерживает файлы с маской BOM. Чтобы устранить проблему, удалите BOM-байты из начала файла, выполнив следующую команду:

```bash
sed -i '1s/^\xEF\xBB\xBF//' accessories.csv
```

3. Имена колонок в файле содержат пробелы, что не совместимо с YDB, поскольку YDB не поддерживает пробелы в именах колонок. Необходимо заменить пробелы в именах колонок, например, на символы подчеркивания. Вы можете сделать это, выполнив следующую команду:

```bash
sed -i '1s/ /_/g' accessories.csv
```

4. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

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

- YDB CLI

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
{% endlist %}

5. Выполните команду импорта:

```bash
ydb import file csv --header --path accessories accessories.csv
```

6. Выполните аналитический запрос, чтобы определить пять самых популярных основных цветов аксессуаров:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      Color_1,
      COUNT(*) AS color_count
  FROM accessories
  GROUP BY Color_1
  ORDER BY color_count DESC
  LIMIT 5;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      Color_1,
      COUNT(*) AS color_count
  FROM accessories
  GROUP BY Color_1
  ORDER BY color_count DESC
  LIMIT 5;'
  ```

{% endlist %}

Пример результата:

```
┌──────────┬─────────────┐
│ Color_1  │ color_count │
├──────────┼─────────────┤
│ "Black"  │ 31          │
├──────────┼─────────────┤
│ "Green"  │ 27          │
├──────────┼─────────────┤
│ "Pink"   │ 20          │
├──────────┼─────────────┤
│ "Red"    │ 20          │
├──────────┼─────────────┤
│ "Yellow" │ 19          │
└──────────┴─────────────┘
```

## Дополнительные ресурсы

- [Документация {{ ydb-short-name }} CLI](https://ydb.tech/ru/docs/reference/ydb-cli/commands/import)
- [Руководство по работе с {{ ydb-short-name }} SQL](https://ydb.tech/ru/docs/yql/reference/)
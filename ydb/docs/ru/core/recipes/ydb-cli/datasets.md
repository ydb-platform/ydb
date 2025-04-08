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

3. **Производительность**: Для больших файлов рекомендуется настраивать [параметры](../../reference/ydb-cli/export-import/import-file.md#optional) `--batch-bytes` и `--max-in-flight` для оптимизации процесса импорта.

## Примеры импорта датасетов

### E-Commerce Behavior Data

Данные о поведении пользователей в мультикатегорийном интернет-магазине.

**Источник**: [Kaggle - E-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data)

**Размер**: 9 GB

**Пример загрузки**:

1. Скачайте и разархивируйте файл `2019-Nov.csv` с Kaggle

2. Датасет включает в себя полностью идентичные строки. Поскольку YDB требует указания уникальных значений первичного ключа, добавим в файл новую колонку под названием `row_id`, где значение ключа будет совпадать с номером строки. Это позволит предотвратить удаление повторяющихся данных. Эту операцию можно осуществить с помощью команды awk:

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

5. Чтобы посмотреть, в какой день было больше всего уникальных пользователей, совершавших покупки, Выполните  запрос:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      DATE(event_time) AS date,
      COUNT(DISTINCT user_id) AS unique_users
  FROM ecommerce_table
  WHERE event_type = 'purchase'
  GROUP BY date
  ORDER BY unique_users DESC
  LIMIT 1;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      DATE(event_time) AS date,
      COUNT(DISTINCT user_id) AS unique_users
  FROM ecommerce_table
  WHERE event_type = "purchase"
  GROUP BY date
  ORDER BY unique_users DESC
  LIMIT 1;'
  ```

{% endlist %}

Этот запрос поможет выявить день с наибольшим количеством уникальных покупателей.

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

Запрос позволит найти, какой издатель достиг наибольшего успеха в Северной Америке по средней продаже.

### COVID-19 Open Research Dataset

Открытый набор данных исследований COVID-19.

**Источник**: [Kaggle - COVID-19 Open Research Dataset Challenge](https://www.kaggle.com/datasets/allen-institute-for-ai/CORD-19-research-challenge?select=metadata.csv)

**Размер**: 1.65 GB (файл metadata.csv)

**Пример загрузки**:

1. Скачайте и разархивируйте файл `metadata.csv` с Kaggle

2. Датасет включает в себя полностью идентичные строки. Поскольку YDB требует указания уникальных значений первичного ключа, добавим в файл новую колонку под названием `row_id`, где значение ключа будет совпадать с номером строки. Это позволит предотвратить удаление повторяющихся данных. Эту операцию можно осуществить с помощью команды awk:

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

5. Чтобы получить количество статей без доступной ссылки на полный текст, выполните запрос:

{% list tabs %} 

- Embedded UI

  ```sql
  SELECT 
      COUNT(*) AS no_full_text_count
  FROM covid_research
  WHERE pdf_json_files IS NULL AND pmc_json_files IS NULL;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      COUNT(*) AS no_full_text_count
  FROM covid_research
  WHERE pdf_json_files IS NULL AND pmc_json_files IS NULL;'
  ```

{% endlist %}

Этот запрос покажет количество исследований, для которых нет полного текста на платформах PDF и PMC.

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

4. Чтобы определить топ-3 стран с наибольшим количеством фильмов и сериалов, добавленных в 2020 году, выполните запрос:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      country, 
      COUNT(*) AS count
  FROM netflix
  WHERE EXTRACT(YEAR FROM DATE(date_added)) = 2020
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
  WHERE EXTRACT(YEAR FROM DATE(date_added)) = 2020
  GROUP BY country
  ORDER BY count DESC
  LIMIT 3;'
  ```

{% endlist %}

Этот запрос покажет, из каких стран было добавлено больше всего контента на Netflix в 2020 году.

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

6. Чтобы посмотреть дорогие товары, доступные для покупки за игровые мили, выполните запрос:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT 
      Name, 
      Miles_Price
  FROM accessories
  WHERE Miles_Price != ''
  ORDER BY CAST(Miles_Price AS Uint64) DESC
  LIMIT 5;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT 
      Name, 
      Miles_Price
  FROM accessories
  WHERE Miles_Price != ""
  ORDER BY CAST(Miles_Price AS Uint64) DESC
  LIMIT 5;'
  ```

{% endlist %}

Этот запрос поможет узнать, какие товары в игре Animal Crossing: New Horizons стоят больше всего игровых миль.

## Дополнительные ресурсы

- [Документация {{ ydb-short-name }} CLI](https://ydb.tech/ru/docs/reference/ydb-cli/commands/import)
- [Руководство по работе с {{ ydb-short-name }} SQL](https://ydb.tech/ru/docs/yql/reference/)
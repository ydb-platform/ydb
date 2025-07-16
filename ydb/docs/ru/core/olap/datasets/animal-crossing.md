# Animal Crossing New Horizons Catalog

{% include [intro](_includes/intro.md) %}

Каталог предметов из популярной игры Animal Crossing: New Horizons.

**Источник**: [Kaggle - Animal Crossing New Horizons Catalog](https://www.kaggle.com/datasets/jessicali9530/animal-crossing-new-horizons-nookplaza-dataset/)

**Размер**: 51 KB

## Пример загрузки

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

## Пример аналитического запроса

Определим пять самых популярных основных цветов аксессуаров:

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

Результат:

```raw
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
# CREATE TABLE (создание таблицы)

{% include [../_includes/alert_preview.md)](../_includes/alert_preview.md) %}

Инструкция `CREATE TABLE` предназначена для создания пустой таблицы в текущей базе данных. Синтаксис команды:

```sql
CREATE [TEMPORARY | TEMP] TABLE <table name> (

<column name> <column data type> [COLLATE][PRIMARY KEY]

[CONSTRAINT  <constraint name> [PRIMARY KEY <column name>],
...]

);
```
При создании таблицы можно задать:
1. **Тип таблицы**: `TEMPORARY` / `TEMP` – временная таблица, которая автоматически удаляется при завершении сессии. Если параметр не задан (оставлен пустым) – создается постоянная таблица. Любые индексы, созданные во временных таблицах, также будут удалены при завершении сессиии, следовательно они тоже являются временными. Допускается существование временной таблицы и постоянной таблицы с одинаковым именем, в этом случае будет выбрана временная таблица.
2. **Имя таблицы**: `<table name>` – можно использовать английские буквы в нижнем регистре, цифры, нижнее подчёркивание и знак доллара ($). Например, название таблицы "People" будет сохранено как "people";
3. **Имя столбца/колонки**: <column name> – действую такие же правила нейминга как и для имен таблиц;
4. **Тип данных**: <column data type> – указываются [стандартные типы](https://www.postgresql.org/docs/current/datatype.html) данных PostgreSQL;
5. **Правило сортировки**: `COLLATE` – [правила сортировки](https://www.postgresql.org/docs/current/collation.html) позволяют устанавливать порядок сортировки и особенности классификации символов в отдельных столбцах или даже при выполнении отдельных операций. К сортируемым типам относятся: `text`, `varchar` и `char`. Можно указать локализацию (`ru_RU`, `en_US`), используемую для определения правил сортировки и сравнения строк в указанных столбцах.
6. Первичный ключ таблицы: `PRIMARY KEY` – обязательное условие при создании таблицы в режиме совместимости YDB с PostgreSQL;
7. Ограничения на уровне таблицы (может быть множество, перечисляются через запятую): `CONSTRAINT` – данный тип ограничения используется как альтернативный синтаксис записи поколоночным ограничениям, или когда нужно задать одинаковые условия ограничения на несколько колонок. Для указания ограничения необходимо указать:
    + Ключевое слово `CONSTRAINT`;
    + Имя ограничения <constraint name>. Правила создания идентификатора для ограничения такие же, как у названия таблиц и названия колонок;
    + Ограничение. Например, `PRIMARY KEY (<column name>)`.


## Создание двух таблиц с первичным ключом и автоинкрементом {#create_table_pk_serial}
#|
|| **Таблица people** | **Таблица social_card** ||
||
```sql
CREATE TABLE people (
    id                 Serial PRIMARY KEY,
    name               Text,
    lastname           Text,
    age                Int,
    country            Text,
    state              Text,
    city               Text,
    birthday           Date,
    sex                Text,
    social_card_number Int
);
```
|
```sql
CREATE TABLE social_card (
    id                   Serial PRIMARY KEY,
    social_card_number   Int,
    card_holder_name     Text,
    card_holder_lastname Text,
    issue                Date,
    expiry               Date,
    issuing_authority    Text,
    category             Text
);
```
||
|#


В этом примере мы использовали псевдотип данных `Serial` –  это удобный и простой способ создать автоинкремент, который автоматически увеличивается на 1 при добавлении новой строки в таблицу.


## Создание таблицы с ограничениями {#create_table_constraint_table}

```sql
CREATE TABLE people (
    id                    Serial,
    name                  Text NOT NULL,
    lastname              Text NOT NULL,
    age                   Int,
    country               Text,
    state                 Text,
    city                  Text,
    birthday              Date,
    sex                   Text NOT NULL,
    social_card_number    Int,
    CONSTRAINT pk PRIMARY KEY(id)
);
```

В этом примере мы создали таблицу "people" с ограничением (блоком `CONSTRAINT`), в котором задали первичный ключ (`PRIMARY KEY`) для колонки "id". Альтернативная запись может выглядеть так: `PRIMARY KEY(id)` без указания ключевого слова `CONSTRAINT`.


## Создание временной таблицы {#create_table_temp_table}

```sql
CREATE TEMPORARY TABLE people (
    id serial PRIMARY KEY,
    name TEXT NOT NULL
);
```

Временная таблица задается через ключевые слова `TEMPORARY` или `TEMP`.


## Создание таблицы с условиями сортировки {#create_table_collate}

```sql
CREATE TABLE people (
    id                   Serial PRIMARY KEY,
    name                 Text COLLATE "en_US",
    lastname             Text COLLATE "en_US",
    age                  Int,
    country              Text,
    state                Text,
    city                 Text,
    birthday             Date,
    sex                  Text,
    social_card_number   Int
);
```

В этом примере колонки "name" и "lastname" используют сортировку с `en_US` локализацией.

{% include [../_includes/alert_locks.md](../_includes/alert_locks.md) %}

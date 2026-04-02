# Полнотекстовые индексы

Полнотекстовые индексы — это специализированный тип [вторичного индекса](../concepts/glossary.md#secondary-index), который позволяет эффективно выполнять поиск по текстовому содержимому в колонках таблицы: по словам, фразам и (с N-граммами) по подстрокам. В отличие от традиционных вторичных индексов, оптимизированных для поиска по равенству или диапазону, полнотекстовые индексы обеспечивают поиск по текстовому содержимому.

Общее описание полнотекстового поиска см. в разделе [Полнотекстовый поиск](../concepts/fulltext_search.md).

## Характеристики полнотекстовых индексов {#characteristics}

Полнотекстовые индексы в {{ ydb-short-name }} строятся путём токенизации текста и создания инвертированного индекса. Это позволяет:

* быстро фильтровать строки через `FulltextMatch()`;
* ранжировать результаты по релевантности ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) через `FulltextScore()` при использовании `fulltext_relevance`;
* применять нормализацию регистра, стемминг и N-граммы с помощью фильтров индекса.

В текущей реализации доступны два варианта индекса:

* `fulltext_plain` — базовый полнотекстовый индекс;
* `fulltext_relevance` — полнотекстовый индекс со статистикой [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) для расчёта релевантности.

Кроме этого, полнотекстовый индекс может быть **покрывающим** (через `COVER`) и включать копию данных дополнительных колонок из основной таблицы.


## Виды полнотекстовых индексов {#types}

### Базовый полнотекстовый индекс (`fulltext_plain`) {#basic}

Глобальный полнотекстовый индекс по колонке `body` для фильтрации через `FulltextMatch()`:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_plain
  ON (body)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Пример запроса к индексу:

```yql
SELECT id, title
FROM articles VIEW ft_index
WHERE FulltextMatch(body, "поисковые термы")
LIMIT 20;
```

### Полнотекстовый индекс для ранжирования (`fulltext_relevance`) {#relevance}

Для ранжирования результатов по релевантности используйте `fulltext_relevance` и функцию `FulltextScore()`:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_relevance
  ON (body) COVER (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Пример запроса с ранжированием:

```yql
SELECT id, title, FulltextScore(body, "поисковые термы") AS relevance
FROM articles VIEW ft_index
WHERE FulltextScore(body, "поисковые термы") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Поиск по подстроке (N-граммы) {#substr}

Если вам нужен поиск по подстроке, создайте индекс с N-граммами. Доступны два типа N-грамм:

* **Обычные N-граммы** (`use_filter_ngram`) — разбивают слова на все возможные подстроки заданной длины, что позволяет находить совпадения в любой части слова. Например, слово "search" будет разбито на "sea", "ear", "arc", "rch" и т.д.
* **Краевые N-граммы** (`use_filter_edge_ngram`) — создают подстроки только от начала слова, что идеально подходит для автодополнения. Например, слово "search" будет разбито на "se", "sea", "sear", "searc", "search".

При использовании N-грамм станут доступны:

* `FulltextMatch(..., "Wildcard" AS Mode)` (шаблоны с `%` / `_`);
* предикаты `LIKE` / `ILIKE` по индексируемой текстовой колонке (используют индекс).

Пример индекса с N-граммами:

```yql
ALTER TABLE articles
  ADD INDEX ngram_index
  GLOBAL USING fulltext_plain
  ON (body)
  WITH (
    tokenizer=standard,
    use_filter_lowercase=true,
    use_filter_ngram=true,
    filter_ngram_min_length=3,
    filter_ngram_max_length=5
  );
```

Пример запроса с `FulltextMatch`:

```yql
SELECT id, title
FROM articles VIEW ngram_index
WHERE FulltextMatch(body, "%обуч%", "Wildcard" AS Mode)
LIMIT 20;
```

Пример запроса с `LIKE`:

```yql
SELECT id, title
FROM articles VIEW ngram_index
WHERE body LIKE "%обуч%ние%"
LIMIT 20;
```

## Полный синтаксис полнотекстовых индексов {#syntax}

Создание полнотекстового индекса:

* при создании таблицы: [CREATE TABLE](../yql/reference/syntax/create_table/fulltext_index.md);
* добавление к существующей таблице: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md).

Полный синтаксис запроса к полнотекстовому индексу:

* [VIEW (Полнотекстовый индекс)](../yql/reference/syntax/select/fulltext_index.md).

Функции и выражения для полнотекстового поиска:

* [Базовые функции полнотекстового поиска](../yql/reference/builtins/fulltext.md);
* [LIKE / ILIKE с полнотекстовым индексом](../yql/reference/syntax/expressions.md#like-ilike-with-fulltext-index).

{% note info %}

Полнотекстовый индекс не будет автоматически выбран оптимизатором, поэтому его нужно указывать явно с помощью `VIEW IndexName`.

Если не использовать выражение `VIEW`, запросы с `FulltextMatch` / `FulltextScore` завершатся с ошибкой.

Это ограничение может быть снято в будущих версиях {{ ydb-short-name }}.

{% endnote %}

## Обновление полнотекстовых индексов {#update}

Полнотекстовые индексы автоматически поддерживаются при модификации данных. Таблицы с полнотекстовыми индексами поддерживают:

* `INSERT`
* `UPSERT`
* `REPLACE`
* `UPDATE`
* `DELETE`

## Удаление полнотекстовых индексов {#drop}

```yql
ALTER TABLE articles DROP INDEX ft_index;
```

## Ограничения {#limitations}

* `BulkUpsert` не поддерживается для таблиц с полнотекстовыми индексами.
* Использование полнотекстового индекса необходимо задавать явно с помощью `VIEW IndexName`.
* В одном полнотекстовом индексе индексируется одна текстовая колонка.
* `FulltextMatch` / `FulltextScore` нельзя использовать c `OR` или `NOT`. Допускается комбинация с другими предикатами через `AND`.
* В одном чтении через `VIEW` поддерживается только один полнотекстовый предикат: нельзя использовать несколько `FulltextScore` и нельзя смешивать `FulltextMatch` и `FulltextScore` в одном `WHERE`.
* Для доступа к индексу по релевантности требуется ограничение `FulltextScore(...) > 0` в `WHERE` (иначе запрос завершится с ошибкой).

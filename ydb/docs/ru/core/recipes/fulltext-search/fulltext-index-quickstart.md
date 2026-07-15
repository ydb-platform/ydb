# Полнотекстовый индекс — быстрый старт

В этом руководстве показано, как создать полнотекстовый индекс и выполнять полнотекстовые запросы в {{ ydb-short-name }}.

## Создайте таблицу и полнотекстовый индекс

```yql
CREATE TABLE articles (
  id Uint64,
  title String,
  body String,
  PRIMARY KEY (id)
);

ALTER TABLE articles ADD INDEX ft_idx
  GLOBAL USING fulltext_relevance
  ON (body) COVER (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

## Добавьте тестовые данные

```yql
UPSERT INTO articles (id, title, body) VALUES
  (1, "Введение", "Машинное обучение — область ИИ"),
  (2, "Поиск",    "Полнотекстовый поиск находит документы по словам"),
  (3, "Заметки",  "Нейронные сети используются в машинном обучении");
```

## Фильтрация документов

```yql
SELECT id, title
FROM articles VIEW ft_idx
WHERE FulltextMatch(body, "машинное обучение")
LIMIT 20;
```

Результат выполнения запроса:

```bash
id title
1  Введение
```

## Ранжирование документов

```yql
SELECT id, title, FulltextScore(body, "машинное обучение") AS relevance
FROM articles VIEW ft_idx
WHERE FulltextScore(body, "машинное обучение") > 0
ORDER BY relevance DESC
LIMIT 10;
```

Результат выполнения запроса:

```bash
id title relevance
1  Введение 1.6215210957338408
```

Подробнее:

* [Полнотекстовые индексы](../../dev/fulltext-indexes.md)
* [VIEW (Полнотекстовый индекс)](../../yql/reference/syntax/select/fulltext_index.md)
* [Базовые функции полнотекстового поиска](../../yql/reference/builtins/fulltext.md)

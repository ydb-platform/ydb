# Функциональный аудит полнотекстового и гибридного поиска

Дата проверки: 2026-09-12.

Проверяемая версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Область проверки: только строковые таблицы. Баги в трекере не создавались.

## Итог

Найдено четыре проблемы реализации и две проблемы документации.

| ID | Тип | Краткое описание | Регрессионный тест |
|---|---|---|---|
| F1 | реализация | Fulltext-индекс не соблюдает read-your-writes | `test_fulltext_index_observes_writes_in_same_transaction` |
| F2 | реализация | N-граммный `LIKE` отклоняет многосегментный шаблон типа `Utf8` | `test_multisegment_like_accepts_literal_of_column_type` |
| F3 | реализация | `Mode="Query"` не исключает термы с префиксом `-` | `test_query_mode_excludes_terms_prefixed_with_minus` |
| F4 | реализация | Hybrid `linear` не работает через Scripting API / `ydb yql` | `test_scripting_api_hybrid_linear_mode_returns_ranked_rows` |
| D1 | документация | Hybrid quickstart не может создать vector-индекс над показанной пустой таблицей | не нужен |
| D2 | документация | В fulltext quickstart устарело точное значение BM25 | не нужен |

## Методика

Сначала сценарии выполнялись вручную обычным CLI на локальном однодисковом YDB-кластере. Затем для каждой проблемы реализации добавлен отдельный regression test:

`ydb/tests/functional/fulltext_hybrid_doc_audit/test_fulltext_hybrid_doc_audit.py`.

Проверялись:

- `fulltext_plain` и `fulltext_relevance`;
- колонки `String` и `Utf8`;
- целочисленный и строковый первичные ключи;
- покрывающий индекс;
- INSERT, UPSERT, UPDATE и DELETE;
- `Keywords`, `Query`, `Wildcard`, `LIKE`, `ILIKE`, `DefaultOperator`, `MinimumShouldMatch`;
- N-граммы и фильтрованные fulltext-индексы с двумя префиксными колонками;
- hybrid RRF, linear, веса, явные `Indexes`/`Limits`, параметризованный `LIMIT`, `RankLambda` и `ScoreLambda`;
- Query API (`ydb sql`) и Scripting API (`ydb yql`).

## Проблемы реализации

### F1. Fulltext-индекс не соблюдает read-your-writes

Документация утверждает, что полнотекстовые индексы автоматически поддерживаются при `INSERT`, `UPSERT`, `UPDATE` и `DELETE`: [fulltext-indexes.md](ydb/docs/ru/core/dev/fulltext-indexes.md#обновление-полнотекстовых-индексов).

В serializable multi-statement query чтение основной таблицы должно видеть предшествующую запись той же транзакции. Обычный синхронный secondary index это гарантирует, но fulltext `VIEW` читает состояние индекса до транзакции:

- `UPSERT`, затем поиск добавленного терма возвращает пустой результат;
- `UPDATE`, затем поиск нового терма возвращает пустой результат, а старый терм остаётся видимым;
- `DELETE`, затем поиск удалённого терма возвращает удалённую строку.

После commit отдельный запрос видит корректное состояние. Ошибка одинаково воспроизводится для `String` и `Utf8`.

Тест: `test_fulltext_index_observes_writes_in_same_transaction`, параметры `operation={insert,update,delete}` и `text_type={String,Utf8}`. Все шесть вариантов падают.

### F2. N-граммный LIKE отклоняет многосегментный Utf8-шаблон

Документация обещает `LIKE`/`ILIKE` для N-граммного индекса и прямо показывает многосегментный шаблон `%обуч%ние%`: [fulltext_index.md](ydb/docs/ru/core/yql/reference/syntax/select/fulltext_index.md#like--ilike-используют-полнотекстовый-индекс).

Для колонки `Utf8` запрос с согласованным по типу литералом:

```yql
SELECT id FROM articles VIEW ft_idx
WHERE body LIKE "%обуч%ние%"u;
```

завершается `BAD_REQUEST`:

```text
Unsupported index access, index name: ft_idx.
FulltextMatch/FulltextScore node is not reachable by conjunctions.
```

Граница проблемы:

- `String`-колонка и String-шаблон проходят;
- `Utf8`-колонка со String-шаблоном проходит;
- `Utf8`-колонка и простой Utf8-шаблон `%обучение%` проходят;
- `Utf8`-колонка и многосегментный Utf8-шаблон `%обуч%ние%` падают;
- явный `FulltextMatch(..., "Wildcard" AS Mode)` проходит.

Тест: `test_multisegment_like_accepts_literal_of_column_type`, параметр `text_type={String,Utf8}`. `String` проходит, `Utf8` падает.

### F3. Mode="Query" не реализует отрицательные термы

Для `FulltextMatch` документация описывает `Mode="Query"` как расширенный синтаксис, где `+` означает обязательный терм, а `-` — исключённый: [fulltext.md](ydb/docs/ru/core/yql/reference/builtins/fulltext.md#fulltextmatch).

При данных:

```text
1: machine learning
2: machine databases
3: databases only
```

запрос:

```yql
WHERE FulltextMatch(body, "+machine -databases", "Query" AS Mode)
```

должен вернуть документ `1`, но возвращает документ `2`. Минимальная проверка `-databases` также возвращает документы, содержащие `databases`: минус удаляется при разборе, но отрицание не применяется. Точные фразы в кавычках работают.

Ошибка воспроизводится для `String` и `Utf8`.

Тест: `test_query_mode_excludes_terms_prefixed_with_minus`, параметр `text_type={String,Utf8}`. Оба варианта падают с фактическим результатом `[2]` вместо `[1]`.

### F4. Hybrid linear падает через Scripting API

Документация объявляет `linear` полноценным режимом `HybridRank`: [hybrid_search.md](ydb/docs/ru/core/yql/reference/syntax/select/hybrid_search.md#режимы-объединения).

Корректный запрос с `fulltext_relevance` и `vector_kmeans_tree` через Scripting API (`ScriptingClient.execute_yql`, CLI-команда `ydb yql`) завершается `GENERIC_ERROR`:

```text
ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3745
TKqpTasksGraph(): requirement resultsSize == 1 failed
```

Граница проблемы:

- тот же `linear` запрос через Query API / `ydb sql` проходит;
- RRF через оба API проходит;
- `linear` с default normalization и `linear` с `Normalize=false` падают через Scripting API;
- ошибка воспроизводится и вручную, и в functional test.

Тест: `test_scripting_api_hybrid_linear_mode_returns_ranked_rows`. Он падает на `ydb.issues.GenericError` с указанной внутренней проверкой.

## Проблемы документации

### D1. Hybrid quickstart содержит неисполняемый DDL vector-индекса

В [hybrid-search.md](ydb/docs/ru/core/dev/hybrid-search.md#подготовка-индексов) сначала создаётся пустая таблица, затем предлагается:

```yql
ALTER TABLE documents
  ADD INDEX vec_idx
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine);
```

На показанной пустой таблице команда падает:

```text
Cannot build vector index: table is empty and
vector_type/vector_dimension were not specified
```

Нужно либо добавить данные до построения индекса, чтобы параметры определились по данным, либо явно указать как минимум `vector_type` и `vector_dimension`. Для детерминированного quickstart предпочтителен второй вариант.

### D2. Fulltext quickstart показывает другое значение BM25

[Quickstart](ydb/docs/ru/core/recipes/fulltext-search/fulltext-index-quickstart.md#ранжирование-документов) для своих точных DDL и данных обещает:

```text
1  Введение 1.6215210957338408
```

Два запуска на проверяемой версии стабильно вернули:

```text
1  Введение 0.9932448131764315
```

Состав и порядок строк правильные; расходится только опубликованное точное значение. Если численное значение BM25 не является стабильной частью контракта, quickstart лучше показывать без точного score либо явно отметить, что число зависит от версии реализации.

## Что прошло

- Quickstart-фильтрация возвращает только статью `id=1`.
- `FulltextScore > 0`, сортировка и покрывающая колонка работают.
- `Keywords` с `And` и `Or`, абсолютный и процентный `MinimumShouldMatch` работают.
- `Wildcard` с `%` и `_` работает на N-граммном индексе.
- Фильтрованный fulltext-индекс с двумя prefix-колонками работает независимо от порядка equality-предикатов.
- Отсутствующий prefix и range вместо equality отклоняются понятной ошибкой.
- Строковый PK автоматически обслуживается через `__ydb_row_id`/`__ydb_unique_row_id`.
- Hybrid RRF вернул ожидаемый порядок `[1, 3, 2, 4]`.
- Hybrid weights, `Indexes`, `Limits`, параметризованный `LIMIT`, `RankLambda` и `ScoreLambda` работают.
- Hybrid read-your-writes для вставки прошёл.

## Результат тестов

Команда:

```bash
./ya make --build relwithdebinfo -tA ydb/tests/functional/fulltext_hybrid_doc_audit
```

Финальный результат: 11 тестовых вариантов, из них один прошёл (`String LIKE`) и десять упали для F1–F4. Сборка и style-check прошли.

ASAN по последнему указанию не использовался.

# Chess Position Evaluations

{% include [intro](_includes/intro.md) %}

Датасет включает 513 миллионов оценок шахматных позиций, выполненных движком Stockfish для анализа на платформе Lichess.

**Источник**: [Kaggle - Chess Position Evaluations](https://www.kaggle.com/datasets/lichess/chess-evaluations)

**Размер**: 59.66 GB

## Пример загрузки

1. Скачайте файл `evals.csv` с Kaggle

2. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `evals` (
      `fen` Text NOT NULL,
      `line` Text NOT NULL,
      `depth` Uint64,
      `knodes` Uint64,
      `cp` Double,
      `mate` Double,
      PRIMARY KEY (`fen`, `line`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `evals` (
      `fen` Text NOT NULL,
      `line` Text NOT NULL,
      `depth` Uint64,
      `knodes` Uint64,
      `cp` Double,
      `mate` Double,
      PRIMARY KEY (`fen`, `line`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );'
  ```

{% endlist %}

3. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path evals evals.csv
```

## Пример аналитического запроса

Определим позиции с наибольшим количеством ходов, проанализированных движком Stockfish:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT
      fen,
      MAX(depth) AS max_depth,
      SUM(knodes) AS total_knodes
  FROM evals
  GROUP BY fen
  ORDER BY max_depth DESC
  LIMIT 10;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT
      fen,
      MAX(depth) AS max_depth,
      SUM(knodes) AS total_knodes
  FROM evals
  GROUP BY fen
  ORDER BY max_depth DESC
  LIMIT 10;'
  ```

{% endlist %}

Этот запрос выполняет следующие действия:

* Находит позиции (представленные в формате FEN) с максимальной глубиной анализа (depth).
* Суммирует количество проанализированных узлов (knodes) для каждой позиции.
* Сортирует результаты по максимальной глубине анализа в порядке убывания.
* Выводит топ-10 позиций с наибольшей глубиной анализа.
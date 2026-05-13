# Формат Protobuf (Value)

Формат по умолчанию. Данные возвращаются построчно в формате [Protobuf](https://protobuf.dev/overview/): каждая строка представляет собой сериализованный набор именованных значений с их YQL-типами. SDK автоматически преобразует полученные значения в нативные типы выбранного языка программирования, предоставляя типобезопасный интерфейс для работы с данными.

Этот формат рекомендуется для:

* Транзакционных ([OLTP](https://ru.wikipedia.org/wiki/OLTP)) задач, где требуется точечное чтение отдельных строк таблицы;
* Приложений, которые обрабатывают небольшие фрагменты таблицы в полном размере;
* Случаев, когда важна нативная интеграция с типами выбранного языка программирования.

## Конвертация YQL-типов {#type-mapping}

Конвертация YQL-типов выполняется в нативные типы языка программирования на стороне SDK и зависит от конкретной его реализации. Подробнее о маппинге типов смотрите в документации используемого SDK.

## Схема возвращаемых данных {#schema}

Схема содержит список столбцов с их YQL-типами. Эта информация достаточна для интерпретации значений на стороне SDK: зная тип столбца, SDK преобразует каждое значение в нативный тип языка программирования.

## Примеры использования в SDK {#sdk-examples}

{% list tabs group=lang %}

- C++

  Данный формат соответствует [`NYdb::TResultSet::EFormat::Value`](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/result/result.h). Он используется по умолчанию на стороне сервиса, если формат не задан; ниже — явное указание, как в примере на Python. Сессию `NYdb::NQuery::TSession` обычно получают в колбэке `TQueryClient::RetryQuerySync` / `RetryQuery`.

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>
  #include <ydb-cpp-sdk/client/types/status/status.h>

  NYdb::TStatus ExampleValue(NYdb::NQuery::TSession session) {
      constexpr std::string_view query = "SELECT * FROM example ORDER BY Key LIMIT 100;";

      auto settings = NYdb::NQuery::TExecuteQuerySettings()
          .Format(NYdb::TResultSet::EFormat::Value)
          .SchemaInclusionMode(NYdb::NQuery::ESchemaInclusionMode::FirstOnly);

      auto queryResult = session.ExecuteQuery(
          query,
          NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
          settings).GetValueSync();

      NYdb::NStatusHelpers::ThrowOnError(queryResult);

      for (const NYdb::TResultSet& resultSet : queryResult.GetResultSets()) {
          std::cout << "Rows: " << resultSet.RowsCount() << ", columns: " << resultSet.ColumnsCount() << std::endl;
      }
  }
  ```

- Python

  Данный формат используется по умолчанию при выполнении запроса через QueryService. Ниже приведён пример явного указания формата возвращаемых данных.

  ```python
  pool = ydb.QuerySessionPool(driver)

  query = """
      SELECT * FROM example ORDER BY Key LIMIT 100;
  """

  result = pool.execute_with_retries(
      query,
      result_set_format=ydb.QueryResultSetFormat.VALUE,
      schema_inclusion_mode=ydb.QuerySchemaInclusionMode.FIRST_ONLY,
  )

  for result_set in result:
      print(f"Record batch with {len(result_set.rows)} rows and {len(result_set.columns)} columns")
  ```

- Java

  Данный формат используется по умолчанию при выполнении запроса через QueryService.

- C#

  Данный формат используется по умолчанию.

{% endlist %}

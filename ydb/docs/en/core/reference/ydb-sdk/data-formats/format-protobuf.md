# Protobuf (Value) format

Default format. Data is returned row by row in the [Protobuf](https://protobuf.dev/overview/) format: each row is a serialized set of named values with their YQL types. The SDK automatically converts the received values into native types of the selected programming language, providing a type-safe interface for working with data.

This format is recommended for:

* Transactional ( [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing)) tasks that require point reads of individual table rows.
* Applications that process small fragments of a table in their entirety.
* Cases where native integration with the types of the selected programming language is important.

## YQL type conversion {#type-mapping}

Conversion of YQL types to native types of the programming language is performed on the SDK side and depends on its specific implementation. For more details on type mapping, see the documentation of the SDK you are using.

## Returned data schema {#schema}

The schema contains a list of columns with their YQL types. This information is sufficient for interpreting values on the SDK side: knowing the column type, the SDK converts each value into a native type of the programming language.

## Usage examples in SDK {#sdk-examples}

{% list tabs group=lang %}

- C++

  This format corresponds to [`NYdb::TResultSet::EFormat::Value`](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/result/result.h). It is used by default on the service side if no format is specified; below is an explicit specification, as in the Python example. The `NYdb::NQuery::TSession` session is usually obtained in the `TQueryClient::RetryQuerySync` / `RetryQuery` callback.


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

  This format is used by default when executing a query through QueryService. Below is an example of explicitly specifying the format of the returned data.


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

  This format is used by default when executing a query through QueryService.
- C#

  This format is used by default.

{% endlist %}

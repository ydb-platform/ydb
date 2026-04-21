# Векторный поиск

В данном разделе содержатся рецепты кода на разных языках программирования для решения задач [векторного поиска](../../concepts/query_execution/vector_search.md) с использованием {{ ydb-short-name }} SDK.

Подробно будут разобраны операции:

* [Подключение к YDB](#connect-ydb)
* [Создание таблицы для хранения векторов](#create-table)
* [Вставка векторов в таблицу](#insert-vectors)
* [Добавление векторного индекса](#add-vector-index)
* [Поиск ближайших векторов](#search-by-vector)

В данном рецепте будет создано хранилище текстов со следующей структурой:

|Поле|Пояснение|
|---|---|
|`id`|Идентификатор текста|
|`document`|Текст|
|`embedding`|Векторное представление текста|

В рецепте предполагается, что `embedding` уже имеется.

## Подключение к {{ ydb-short-name }} {#connect-ydb}

В данной секции описаны минимально необходимые действия для выполнения запросов в {{ ydb-short-name }}.
Для получения более подробной информации о подключении к {{ ydb-short-name }} обратитесь к статье [{#T}](./init.md).

{% list tabs %}

- C++

  ```cpp
  #include <ydb-cpp-sdk/client/query/client.h>
  #include <ydb-cpp-sdk/client/query/client.h>

  int main() {
      auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
      NYdb::TDriver driver(driverConfig);
      NYdb::NQuery::TQueryClient client(driver);

      // ...
  }
  ```

- Go

    ```go
    package main

    import (
      "context"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
    }
    ```

- Python

    {% list tabs %}

    - Native SDK

      Для выполнения запросов необходимо создать `ydb.QuerySessionPool`.

      ```python
      import ydb

      driver = ydb.Driver(
          endpoint=ydb_endpoint,
          database=ydb_database,
          credentials=ydb_credentials,
      )
      driver.wait(5, fail_fast=True)
      pool = ydb.QuerySessionPool(driver)
      ```

    - Native SDK (Asyncio)

      Для выполнения запросов необходимо создать `ydb.aio.QuerySessionPool`:

      ```python
      import asyncio
      import ydb

      async def main():
          async with ydb.aio.Driver(
              endpoint=ydb_endpoint,
              database=ydb_database,
              credentials=ydb_credentials,
          ) as driver:
              await driver.wait(5, fail_fast=True)
              pool = ydb.aio.QuerySessionPool(driver)
              # ... используйте pool ...

      asyncio.run(main())
      ```

    {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'
  import { query, unsafe, identifier } from '@ydbjs/query'

  const driver = new Driver('grpc://localhost:2136/local')
  await driver.ready()
  const sql = query(driver)
  ```

- Java

    Для запросов используйте `QueryClient` и `SessionRetryContext` (см. [инициализацию драйвера](./init.md)). Ниже — минимальное подключение и создание клиента для YQL Query Service:

    ```java
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.tools.SessionRetryContext;

    String connectionString = System.getenv().getOrDefault("YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

    try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
       QueryClient queryClient = QueryClient.newClient(transport).build()) {

      SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
      // retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(...)))
    }
    ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}


## Создание таблицы {#create-table}

Сначала необходимо создать таблицу для хранения документов и их векторных представлений.

Структура таблицы:

|Название столбца|Тип данных|Пояснение|
|---|----|------|
|`id`|`Utf8`|идентификатор документа|
|`document`|`Utf8`|текст документа|
|`embedding`|`String`|векторное представление документа|

{% note warning %}

Для хранения вектора используется тип `String`. Подробнее смотрите в документации по [точному векторному поиску](../../yql/reference/udf/list/knn.md#data-types).

{% endnote %}


{% list tabs %}

- C++

    ```cpp
    void CreateVectorTable(NYdb::NQuery::TQueryClient& client, const std::string& tableName)
    {
        std::string query = std::format(R"(
            CREATE TABLE IF NOT EXISTS `{}` (
                id Utf8,
                document Utf8,
                embedding String,
                PRIMARY KEY (id)
            ))", tableName);

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([&](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        }));

        std::cout << "Vector table created: " << tableName << std::endl;
    }
    ```

- Go

    ```go
    func createVectorTable(ctx context.Context, db *ydb.Driver, tableName string) error {
      query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
          id Utf8,
          document Utf8,
          embedding String,
          PRIMARY KEY (id)
        );`, "`"+tableName+"`")

      return db.Query().Exec(ctx, query)
    }
    ```

- Python

    {% list tabs %}

    - Native SDK

      ```python
      import ydb

      def create_vector_table(pool: ydb.QuerySessionPool, table_name: str) -> None:
          query = f"""
          CREATE TABLE IF NOT EXISTS `{table_name}` (
              id Utf8,
              document Utf8,
              embedding String,
              PRIMARY KEY (id)
          );"""

          pool.execute_with_retries(query)

          print(f"Vector table {table_name} created")
      ```

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def create_vector_table(pool: ydb.aio.QuerySessionPool, table_name: str) -> None:
          query = f"""
          CREATE TABLE IF NOT EXISTS `{table_name}` (
              id Utf8,
              document Utf8,
              embedding String,
              PRIMARY KEY (id)
          );"""

          await pool.execute_with_retries(query)

          print(f"Vector table {table_name} created")
      ```

    {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  await sql`CREATE TABLE IF NOT EXISTS `table_name` (
    id Utf8,
    document Utf8,
    embedding String,
    PRIMARY KEY (id)
  );`
  ```

- Java

  ```java
  import tech.ydb.common.transaction.TxMode;
  import tech.ydb.query.tools.QueryReader;
  import tech.ydb.query.tools.SessionRetryContext;
  import tech.ydb.table.query.Params;

  void createVectorTable(SessionRetryContext retryCtx, String tableName) {
      String query = String.format("""
              CREATE TABLE IF NOT EXISTS `%s` (
                  id Utf8,
                  document Utf8,
                  embedding String,
                  PRIMARY KEY (id)
              );""", tableName);

      retryCtx.supplyResult(session -> QueryReader.readFrom(
              session.createQuery(query, TxMode.NONE, Params.empty())
      )).join().getValue();

      System.out.println("Vector table created: " + tableName);
  }
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}


## Вставка векторов {#insert-vectors}

Для вставки векторов необходимо подготовить и выполнить правильный YQL-запрос. Для унификации вставки разных данных он параметризован.

Запрос оперирует контейнерным типом данных `List<Struct<...>>` (список структур), что позволяет передавать через параметры произвольное количество объектов за один раз.

В {{ ydb-short-name }} таблицах же вектора хранятся в виде сериализованной последовательности байт. Конвертацию в такое представление **рекомендуется выполнять на клиенте**. Альтернативный способ — делегировать конвертацию на сервер с помощью функции преобразования [Knn UDF](../../yql/reference/udf/list/knn.md#functions-convert). Ниже будут приведены примеры, демонстрирующие оба подхода.

{% list tabs %}

- C++

    ```cpp
    std::string ConvertVectorToBytes(const std::vector<float>& vector)
    {
        std::string result;
        for (const auto& value : vector) {
            const char* bytes = reinterpret_cast<const char*>(&value);
            result += std::string(bytes, sizeof(float));
        }
        return result + "\x01";
    }

    void InsertItemsAsBytes(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<TItem>& items)
    {
        std::string query = std::format(R"(
            DECLARE $items AS List<Struct<
                id: Utf8,
                document: Utf8,
                embedding: String
            >>;
            UPSERT INTO `{0}`
            (
                id,
                document,
                embedding
            )
            SELECT
                id,
                document,
                embedding,
            FROM AS_TABLE($items);
        )", tableName);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$items");
        valueBuilder.BeginList();
        for (const auto& item : items) {
            valueBuilder.AddListItem();
            valueBuilder.BeginStruct();
            valueBuilder.AddMember("id").Utf8(item.Id);
            valueBuilder.AddMember("document").Utf8(item.Document);
            valueBuilder.AddMember("embedding").String(ConvertVectorToBytes(item.Embedding));
            valueBuilder.EndStruct();
        }
        valueBuilder.EndList();
        valueBuilder.Build();

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        }));

        std::cout << items.size() << " items inserted" << std::endl;
    }
    ```

    {% note info %}

    В функции `ConvertVectorToBytes` подразумевается, что на клиенте используется процессор с [little-endian порядком байт](https://ru.wikipedia.org/wiki/Порядок_байтов), например x86\_64. Если используется другой порядок байт, функцию `ConvertVectorToBytes` необходимо адаптировать.

    {% endnote %}

- Go

    Функция конвертирует вектор float32 в бинарное представление и выполняет параметризованный запрос:

    ```go
    import (
      "encoding/binary"
      "math"
    )

    func convertVectorToBytes(vector []float32) []byte {
      buf := make([]byte, len(vector)*4+1)
      for i, v := range vector {
        binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
      }
      buf[len(buf)-1] = 0x01
      return buf
    }

    func insertItems(ctx context.Context, db *ydb.Driver, tableName string, items []Item) error {
      query := fmt.Sprintf(`
        DECLARE $items AS List<Struct<
          id: Utf8,
          document: Utf8,
          embedding: String
        >>;

        UPSERT INTO %s
        (id, document, embedding)
        SELECT id, document, embedding
        FROM AS_TABLE($items);
      `, "`"+tableName+"`")

      rows := make([]types.Value, 0, len(items))
      for _, item := range items {
        rows = append(rows, types.StructValue(
          types.StructFieldValue("id", types.UTF8Value(item.ID)),
          types.StructFieldValue("document", types.UTF8Value(item.Document)),
          types.StructFieldValue("embedding", types.BytesValue(convertVectorToBytes(item.Embedding))),
        ))
      }

      return db.Query().Exec(ctx, query,
        query.WithParameters(
          ydb.ParamsBuilder().Param("$items").BeginList().AddItems(rows...).EndList().Build(),
        ),
      )
    }
    ```

- Python

    Метод принимает массив словарей `items`, где каждый словарь содержит поля `id` - идентификатор, `document` - текст, `embedding` - векторное представление текста, заранее сериализованное в последовательность байт.

    Для использования структуры в примере ниже создается `items_struct_type = ydb.StructType()`, в котором задаются типы всех полей. Для передачи списка таких структур его необходимо обернуть в `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    {% list tabs %}
    - Native SDK

      ```python
      import struct
      import ydb


      def convert_vector_to_bytes(vector: list[float]) -> bytes:
          b = struct.pack("f" * len(vector), *vector)
          return b + b"\x01"

      def insert_items_vector_as_bytes(
          pool: ydb.QuerySessionPool,
          table_name: str,
          items: list[dict],
      ) -> None:
          query = f"""
          DECLARE $items AS List<Struct<
              id: Utf8,
              document: Utf8,
              embedding: String
          >>;

          UPSERT INTO `{table_name}`
          (
              id,
              document,
              embedding
          )
          SELECT
              id,
              document,
              embedding,
          FROM AS_TABLE($items);
          """

          items_struct_type = ydb.StructType()
          items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("embedding", ydb.PrimitiveType.String)

          for item in items:
              item["embedding"] = convert_vector_to_bytes(item["embedding"])

          pool.execute_with_retries(
              query, {"$items": (items, ydb.ListType(items_struct_type))}
          )

          print(f"{len(items)} items inserted")
      ```

    - Native SDK (Asyncio)

      ```python
      import struct
      import ydb

      def convert_vector_to_bytes(vector: list[float]) -> bytes:
          b = struct.pack("f" * len(vector), *vector)
          return b + b"\x01"

      async def insert_items_vector_as_bytes(
          pool: ydb.aio.QuerySessionPool,
          table_name: str,
          items: list[dict],
      ) -> None:
          query = f"""
          DECLARE $items AS List<Struct<
              id: Utf8,
              document: Utf8,
              embedding: String
          >>;

          UPSERT INTO `{table_name}`
          (
              id,
              document,
              embedding
          )
          SELECT
              id,
              document,
              embedding,
          FROM AS_TABLE($items);
          """

          items_struct_type = ydb.StructType()
          items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("embedding", ydb.PrimitiveType.String)

          for item in items:
              item["embedding"] = convert_vector_to_bytes(item["embedding"])

          await pool.execute_with_retries(
              query, {"$items": (items, ydb.ListType(items_struct_type))}
          )

          print(f"{len(items)} items inserted")
      ```

    {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  function convertVectorToBytes(vector) {
    const bytes = new Uint8Array(vector.length * 4 + 1);
    const view = new DataView(bytes.buffer);

    for (let i = 0; i < vector.length; i++) {
        view.setFloat32(i * 4, vector[i], true);
    }

    bytes[bytes.length - 1] = 0x01;
    return bytes;
  }

  const items = [
    {
      id: "first_doc",
      document: "My Document",
      embedding: convertVectorToBytes(new Float32Array([1.5, 2.5, 3.5]))
    }
  ]

  await sql`
    UPSERT INTO `table_name` (id, document, embedding)
    SELECT id, document, embedding,
    FROM AS_TABLE($items);`
  ```

- Java

    ```java
    import java.nio.ByteBuffer;
    import java.nio.ByteOrder;
    import java.util.ArrayList;
    import java.util.List;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.values.ListType;
    import tech.ydb.table.values.ListValue;
    import tech.ydb.table.values.PrimitiveType;
    import tech.ydb.table.values.PrimitiveValue;
    import tech.ydb.table.values.StructType;
    import tech.ydb.table.values.Value;

    byte[] convertVectorToBytes(float[] vector) {
        ByteBuffer bb = ByteBuffer.allocate(vector.length * Float.BYTES + 1).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            bb.putFloat(v);
        }
        bb.put((byte) 0x01);
        return bb.array();
    }

    void insertItemsAsBytes(SessionRetryContext retryCtx, String tableName, List<Item> items) {
        String query = String.format("""
                DECLARE $items AS List<Struct<
                    id: Utf8,
                    document: Utf8,
                    embedding: String
                >>;

                UPSERT INTO `%s`
                (
                    id,
                    document,
                    embedding
                )
                SELECT
                    id,
                    document,
                    embedding,
                FROM AS_TABLE($items);""", tableName);

        StructType rowType = StructType.of(
                "id", PrimitiveType.Text,
                "document", PrimitiveType.Text,
                "embedding", PrimitiveType.Bytes
        );

        List<Value<?>> rows = new ArrayList<>(items.size());
        for (Item item : items) {
            rows.add(rowType.newValue(
                    "id", PrimitiveValue.newText(item.id()),
                    "document", PrimitiveValue.newText(item.document()),
                    "embedding", PrimitiveValue.newBytes(convertVectorToBytes(item.embedding()))
            ));
        }

        ListValue itemsParam = ListType.of(rowType).newValue(rows);
        Params params = Params.of("$items", itemsParam);

        retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(query, TxMode.SERIALIZABLE_RW, params)
        )).join().getValue();

        System.out.println(items.size() + " items inserted");
    }

    // record Item(String id, String document, float[] embedding) {}
    ```

    {% note info %}

    В функции `ConvertVectorToBytes` подразумевается, что на клиенте используется процессор с [little-endian порядком байт](https://ru.wikipedia.org/wiki/Порядок_байтов), например x86\_64. Если используется другой порядок байт, функцию `ConvertVectorToBytes` необходимо адаптировать.

    {% endnote %}

    Вариант с передачей компонентов вектора как `List<Float>` и преобразованием на стороне YQL через `Knn::ToBinaryStringFloat` — по той же схеме, что для альтернативных примеров на Python и C++ ниже:

    ```java
    import java.util.ArrayList;
    import java.util.List;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.values.ListType;
    import tech.ydb.table.values.ListValue;
    import tech.ydb.table.values.PrimitiveType;
    import tech.ydb.table.values.PrimitiveValue;
    import tech.ydb.table.values.StructType;
    import tech.ydb.table.values.Value;

    void insertItemsAsFloatList(SessionRetryContext retryCtx, String tableName, List<Item> items) {
        String query = String.format("""
                DECLARE $items AS List<Struct<
                    id: Utf8,
                    document: Utf8,
                    embedding: List<Float>
                >>;

                UPSERT INTO `%s`
                (
                    id,
                    document,
                    embedding
                )
                SELECT
                    id,
                    document,
                    Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
                FROM AS_TABLE($items);""", tableName);

        StructType rowType = StructType.of(
                "id", PrimitiveType.Text,
                "document", PrimitiveType.Text,
                "embedding", ListType.of(PrimitiveType.Float)
        );

        List<Value<?>> rows = new ArrayList<>(items.size());
        for (Item item : items) {
            Value<?>[] emb = new Value<?>[item.embedding().length];
            for (int i = 0; i < item.embedding().length; i++) {
                emb[i] = PrimitiveValue.newFloat(item.embedding()[i]);
            }
            ListValue embList = ListType.of(PrimitiveType.Float).newValueOwn(emb);
            rows.add(rowType.newValue(
                    "id", PrimitiveValue.newText(item.id()),
                    "document", PrimitiveValue.newText(item.document()),
                    "embedding", embList
            ));
        }

        ListValue itemsParam = ListType.of(rowType).newValue(rows);
        Params params = Params.of("$items", itemsParam);

        retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(query, TxMode.SERIALIZABLE_RW, params)
        )).join().getValue();

        System.out.println(items.size() + " items inserted");
    }

    // record Item(String id, String document, float[] embedding) {}
    ```

- C++ (альтернативный)

    ```cpp
    void InsertItemsAsFloatList(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<TItem>& items)
    {
        std::string query = std::format(R"(
            DECLARE $items AS List<Struct<
                id: Utf8,
                document: Utf8,
                embedding: List<Float>
            >>;

            UPSERT INTO `{}`
            (
                id,
                document,
                embedding
            )
            SELECT
                id,
                document,
                Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
            FROM AS_TABLE($items);
        )", tableName);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$items");
        valueBuilder.BeginList();
        for (const auto& item : items) {
            valueBuilder.AddListItem();
            valueBuilder.BeginStruct();
            valueBuilder.AddMember("id").Utf8(item.Id);
            valueBuilder.AddMember("document").Utf8(item.Document);
            valueBuilder.AddMember("embedding").BeginList();
            for (const auto& value : item.Embedding) {
                valueBuilder.AddListItem().Float(value);
            }
            valueBuilder.EndList();
            valueBuilder.EndStruct();
        }
        valueBuilder.EndList();
        valueBuilder.Build();

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
        }));

        std::cout << items.size() << " items inserted" << std::endl;
    }
    ```

- Python (альтернативный)

    Метод принимает массив словарей `items`, где каждый словарь содержит поля `id` - идентификатор, `document` - текст, `embedding` - векторное представление текста.

    Для использования структуры в примере ниже создается `items_struct_type = ydb.StructType()`, в котором задаются типы всех полей. Для передачи списка таких структур его необходимо обернуть в `ydb.ListType`: `ydb.ListType(items_struct_type)`.

    {% list tabs %}
    - Native SDK

      ```python
      import ydb

      def insert_items_vector_as_float_list(
          pool: ydb.QuerySessionPool,
          table_name: str,
          items: list[dict],
      ) -> None:
          query = f"""
          DECLARE $items AS List<Struct<
              id: Utf8,
              document: Utf8,
              embedding: List<Float>
          >>;

          UPSERT INTO `{table_name}`
          (
              id,
              document,
              embedding
          )
          SELECT
              id,
              document,
              Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
          FROM AS_TABLE($items);
          """

          items_struct_type = ydb.StructType()
          items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("embedding", ydb.ListType(ydb.PrimitiveType.Float))

          pool.execute_with_retries(
              query, {"$items": (items, ydb.ListType(items_struct_type))}
          )

          print(f"{len(items)} items inserted")
      ```

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def insert_items_vector_as_float_list(
          pool: ydb.aio.QuerySessionPool,
          table_name: str,
          items: list[dict],
      ) -> None:
          query = f"""
          DECLARE $items AS List<Struct<
              id: Utf8,
              document: Utf8,
              embedding: List<Float>
          >>;

          UPSERT INTO `{table_name}`
          (
              id,
              document,
              embedding
          )
          SELECT
              id,
              document,
              Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
          FROM AS_TABLE($items);
          """

          items_struct_type = ydb.StructType()
          items_struct_type.add_member("id", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("document", ydb.PrimitiveType.Utf8)
          items_struct_type.add_member("embedding", ydb.ListType(ydb.PrimitiveType.Float))

          await pool.execute_with_retries(
              query, {"$items": (items, ydb.ListType(items_struct_type))}
          )

          print(f"{len(items)} items inserted")
      ```

    {% endlist %}

- JavaScript (альтернативный)

  ```javascript
  const items = [
    {
      id: "first_doc",
      document: "My Document",
      embedding: new Float32Array([1.5, 2.5, 3.5])
    }
  ]

  await sql`
    UPSERT INTO `table_name` (id, document, embedding)
    SELECT id, document, Untag(Knn::ToBinaryStringFloat(embedding), "FloatVector"),
    FROM AS_TABLE($items);`
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}


## Добавление индекса {#add-vector-index}

Использование векторного индекса позволяет эффективно решать задачу приближённого поиска ближайших векторов. Подробнее о преимуществах и особенностях использования описано в документации по [векторному индексу](../../dev/vector-indexes.md).

Для добавления индекса необходимо выполнить две операции:

1. Создать временный индекс;
2. Сохранить временный индекс как постоянный.

Такой подход позволяет создавать индекс как при его первоначальном создании, так и при перестроении (если индекс уже существует).

Доступные стратегии:

* `similarity=cosine`;
* `similarity=inner_product`;
* `distance=cosine`;
* `distance=euclidean`;
* `distance=manhattan`.

Каждая стратегия определяет функцию, которая будет использоваться для последующего поиска. Более подробно функции описаны в документации по [функциям расстояния и сходства](../../yql/reference/udf/list/knn.md#fuctions-distance).

Параметры, применяемые при создании индекса типа `vector_kmeans_tree`, описаны в документации [векторного индекса](../../dev/vector-indexes.md#kmeans-tree-type).


{% list tabs %}

- C++

    ```cpp
    void AddIndex(
        NYdb::TDriver& driver,
        NYdb::NQuery::TQueryClient& client,
        const std::string& database,
        const std::string& tableName,
        const std::string& indexName,
        const std::string& strategy,
        std::uint64_t dim,
        std::uint64_t levels,
        std::uint64_t clusters)
    {
        std::string query = std::format(R"(
            ALTER TABLE `{0}`
            ADD INDEX {1}__temp
            GLOBAL USING vector_kmeans_tree
            ON (embedding)
            WITH (
                {2},
                vector_type="Float",
                vector_dimension={3},
                levels={4},
                clusters={5},
                overlap_clusters=3
            );
        )", tableName, indexName, strategy, dim, levels, clusters);

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([&](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        }));

        NYdb::NTable::TTableClient tableClient(driver);
        NYdb::NStatusHelpers::ThrowOnError(tableClient.RetryOperationSync([&](NYdb::NTable::TSession session) {
            return session.AlterTable(database + "/" + tableName, NYdb::NTable::TAlterTableSettings()
                .AppendRenameIndexes(NYdb::NTable::TRenameIndex{
                    .SourceName_ = indexName + "__temp",
                    .DestinationName_ = indexName,
                    .ReplaceDestination_ = true
                })
            ).ExtractValueSync();
        }));

        std::cout << "Table index `" << indexName << "` for table `" << tableName << "` added" << std::endl;
    }
    ```

- Go

    ```go
    func addVectorIndex(
      ctx context.Context,
      db *ydb.Driver,
      tableName, indexName, strategy string,
      dimension, levels, clusters int,
    ) error {
      tempIndexName := indexName + "__temp"
      query := fmt.Sprintf(`
        ALTER TABLE %s
        ADD INDEX %s
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
          %s,
          vector_type="Float",
          vector_dimension=%d,
          levels=%d,
          clusters=%d
        );
      `, "`"+tableName+"`", tempIndexName, strategy, dimension, levels, clusters)

      if err := db.Query().Exec(ctx, query); err != nil {
        return err
      }

      return db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
        return s.AlterTable(ctx, path.Join(db.Name(), tableName),
          options.WithRenameIndex(tempIndexName, indexName, true),
        )
      })
    }
    ```

- Python

    {% cut "asyncio" %}

    ```python
    import ydb

    async def add_vector_index(
        pool: ydb.aio.QuerySessionPool,
        driver: ydb.aio.Driver,
        table_name: str,
        index_name: str,
        strategy: str,
        dimension: int,
        levels: int = 2,
        clusters: int = 128,
    ):
        temp_index_name = f"{index_name}__temp"
        query = f"""
        ALTER TABLE `{table_name}`
        ADD INDEX {temp_index_name}
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
            {strategy},
            vector_type="Float",
            vector_dimension={dimension},
            levels={levels},
            clusters={clusters},
            overlap_clusters=3
        );
        """

        await pool.execute_with_retries(query)
        await driver.table_client.alter_table(
            f"{driver._driver_config.database}/{table_name}",
            rename_indexes=[
                ydb.RenameIndexItem(
                    source_name=temp_index_name,
                    destination_name=f"{index_name}",
                    replace_destination=True,
                ),
            ],
        )

      ```python
      import ydb

    {% endcut %}

    ```python
    def add_vector_index(
        pool: ydb.QuerySessionPool,
        driver: ydb.Driver,
        table_name: str,
        index_name: str,
        strategy: str,
        dimension: int,
        levels: int = 2,
        clusters: int = 128,
    ):
        temp_index_name = f"{index_name}__temp"
        query = f"""
        ALTER TABLE `{table_name}`
        ADD INDEX {temp_index_name}
        GLOBAL USING vector_kmeans_tree
        ON (embedding)
        WITH (
            {strategy},
            vector_type="Float",
            vector_dimension={dimension},
            levels={levels},
            clusters={clusters},
            overlap_clusters=3
        );
        """

        pool.execute_with_retries(query)
        driver.table_client.alter_table(
            f"{driver._driver_config.database}/{table_name}",
            rename_indexes=[
                ydb.RenameIndexItem(
                    source_name=temp_index_name,
                    destination_name=f"{index_name}",
                    replace_destination=True,
                ),
            ],
        )

          pool.execute_with_retries(query)
          driver.table_client.alter_table(
              f"{driver._driver_config.database}/{table_name}",
              rename_indexes=[
                  ydb.RenameIndexItem(
                      source_name=temp_index_name,
                      destination_name=f"{index_name}",
                      replace_destination=True,
                  ),
              ],
          )

          print(f"Table index {index_name} created.")
      ```

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def add_vector_index(
          pool: ydb.aio.QuerySessionPool,
          driver: ydb.aio.Driver,
          table_name: str,
          index_name: str,
          strategy: str,
          dimension: int,
          levels: int = 2,
          clusters: int = 128,
      ):
          temp_index_name = f"{index_name}__temp"
          query = f"""
          ALTER TABLE `{table_name}`
          ADD INDEX {temp_index_name}
          GLOBAL USING vector_kmeans_tree
          ON (embedding)
          WITH (
              {strategy},
              vector_type="Float",
              vector_dimension={dimension},
              levels={levels},
              clusters={clusters}
          );
          """

          await pool.execute_with_retries(query)
          await driver.table_client.alter_table(
              f"{driver._driver_config.database}/{table_name}",
              rename_indexes=[
                  ydb.RenameIndexItem(
                      source_name=temp_index_name,
                      destination_name=f"{index_name}",
                      replace_destination=True,
                  ),
              ],
          )

          print(f"Table index {index_name} created.")
      ```

    {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

    ```java
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.settings.AlterTableSettings;

    void addVectorIndex(
            GrpcTransport transport,
            SessionRetryContext queryRetry,
            SessionRetryContext tableRetry,
            String tableName,
            String indexName,
            String strategy,
            long dimension,
            long levels,
            long clusters) {

        String tempIndexName = indexName + "__temp";
        String query = String.format("""
                ALTER TABLE `%s`
                ADD INDEX %s
                GLOBAL USING vector_kmeans_tree
                ON (embedding)
                WITH (
                    %s,
                    vector_type="Float",
                    vector_dimension=%d,
                    levels=%d,
                    clusters=%d
                );
                """, tableName, tempIndexName, strategy, dimension, levels, clusters);

        queryRetry.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(query, TxMode.NONE, Params.empty())
        )).join().getValue();

        String tablePath = transport.getDatabase() + "/" + tableName;
        AlterTableSettings settings = new AlterTableSettings()
                .addRenameIndex(tempIndexName, indexName, true);

        tableRetry.supplyStatus(session -> session.alterTable(tablePath, settings))
                .join()
                .expectSuccess("alter table rename index");

        System.out.println("Table index `" + indexName + "` for table `" + tableName + "` added");
    }

    // SessionRetryContext tableRetry = SessionRetryContext.create(TableClient.newClient(transport).build()).build();
    ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Поиск по вектору {#search-by-vector}

Для поиска документов по вектору используется специальный YQL‑запрос, в котором необходимо определить функцию сходства или расстояния.
Доступные значения:

* `CosineSimilarity`;
* `InnerProductSimilarity`;
* `CosineDistance`;
* `ManhattanDistance`;
* `EuclideanDistance`.

Подробнее функции описаны в документации по [функциям расстояния и сходства](../../yql/reference/udf/list/knn.md#fuctions-distance).

Метод позволяет указать имя индекса. Если оно задано, в запрос будет добавлено выражение `VIEW index_name`, что позволит использовать векторный индекс при поиске.

Метод возвращает список, состоящий из словарей с полями `id`, `document`, а также `score` — числом, отражающим степень сходства (или расстояния) с искомым вектором.

{% list tabs %}

- C++

    ```cpp
    std::vector<TResultItem> SearchItemsAsBytes(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<float>& embedding,
        const std::string& strategy,
        std::uint64_t limit,
        std::uint64_t topClusters = 10,
        const std::optional<std::string>& indexName = std::nullopt)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            PRAGMA ydb.KMeansTreeSearchTopSize = "{5}";
            DECLARE $embedding as String;
            SELECT
                id,
                document,
                Knn::{2}(embedding, $embedding) as score
            FROM {0} {1}
            ORDER BY score {3}
            LIMIT {4};
        )", tableName, viewIndex, strategy, sortOrder, limit, topClusters);

        auto params = NYdb::TParamsBuilder()
            .AddParam("$embedding")
                .String(ConvertVectorToBytes(embedding))
                .Build()
            .Build();

        std::vector<TResultItem> result;

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params, &query, &result](NYdb::NQuery::TSession session) {
            auto execResult = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            if (execResult.IsSuccess()) {
                auto parser = execResult.GetResultSetParser(0);
                while (parser.TryNextRow()) {
                    result.push_back({
                        .Id = *parser.ColumnParser(0).GetOptionalUtf8(),
                        .Document = *parser.ColumnParser(1).GetOptionalUtf8(),
                        .Score = *parser.ColumnParser(2).GetOptionalFloat()
                    });
                }
            }
            return execResult;
        }));

        return result;
    }
    ```

- Go

    ```go
    type ResultItem struct {
      ID       string
      Document string
      Score    float32
    }

    func searchItems(
      ctx context.Context,
      db *ydb.Driver,
      tableName string,
      embedding []float32,
      strategy string,
      limit int,
      indexName string,
    ) ([]ResultItem, error) {
      viewIndex := ""
      if indexName != "" {
        viewIndex = "VIEW " + indexName
      }
      sortOrder := "DESC"
      if !strings.HasSuffix(strategy, "Similarity") {
        sortOrder = "ASC"
      }
      q := fmt.Sprintf(`
        DECLARE $embedding AS String;
        SELECT id, document, Knn::%s(embedding, $embedding) AS score
        FROM %s %s
        ORDER BY score %s
        LIMIT %d;
      `, strategy, tableName, viewIndex, sortOrder, limit)

      row, err := db.Query().Query(ctx, q,
        query.WithParameters(
          ydb.ParamsBuilder().Param("$embedding").Bytes(convertVectorToBytes(embedding)).Build(),
        ),
      )
      if err != nil {
        return nil, err
      }
      defer row.Close(ctx)

      var items []ResultItem
      for rs, err := row.NextResultSet(ctx); err == nil; rs, err = row.NextResultSet(ctx) {
        for r, err := rs.NextRow(ctx); err == nil; r, err = rs.NextRow(ctx) {
          var item ResultItem
          if err := r.ScanNamed(
            query.Named("id", &item.ID),
            query.Named("document", &item.Document),
            query.Named("score", &item.Score),
          ); err != nil {
            return nil, err
          }
          items = append(items, item)
        }
      }
      return items, nil
    }
    ```

- Python

    {% list tabs %}

    - Native SDK

      ```python
      def search_items_vector_as_bytes(
          pool: ydb.QuerySessionPool,
          table_name: str,
          embedding: list[float],
          strategy: str = "CosineSimilarity",
          limit: int = 1,
          index_name: str | None = None,
          top_clusters: int = 10,
      ) -> list[dict]:
          view_index = f"VIEW {index_name}" if index_name else ""

          sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

          query = f"""
          PRAGMA ydb.KMeansTreeSearchTopSize = "{top_clusters}";
          DECLARE $embedding as String;

          SELECT
              id,
              document,
              Knn::{strategy}(embedding, $embedding) as score
          FROM {table_name} {view_index}
          ORDER BY score {sort_order}
          LIMIT {limit};
          """

          result = pool.execute_with_retries(
              query,
              {
                  "$embedding": (
                      convert_vector_to_bytes(embedding),
                      ydb.PrimitiveType.String,
                  ),
              },
          )

          items = []

          for result_set in result:
              for row in result_set.rows:
                  items.append(
                      {
                          "id": row["id"],
                          "document": row["document"],
                          "score": row["score"],
                      }
                  )

          return items
      ```

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def search_items_vector_as_bytes(
          pool: ydb.aio.QuerySessionPool,
          table_name: str,
          embedding: list[float],
          strategy: str = "CosineSimilarity",
          limit: int = 1,
          index_name: str | None = None,
      ) -> list[dict]:
          view_index = f"VIEW {index_name}" if index_name else ""

          sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

          query = f"""
          DECLARE $embedding as String;

          SELECT
              id,
              document,
              Knn::{strategy}(embedding, $embedding) as score
          FROM {table_name} {view_index}
          ORDER BY score {sort_order}
          LIMIT {limit};
          """

          result = await pool.execute_with_retries(
              query,
              {
                  "$embedding": (
                      convert_vector_to_bytes(embedding),
                      ydb.PrimitiveType.String,
                  ),
              },
          )

          items = []

          for result_set in result:
              for row in result_set.rows:
                  items.append(
                      {
                          "id": row["id"],
                          "document": row["document"],
                          "score": row["score"],
                      }
                  )

          return items
      ```

    {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  const limit;
  const embedding = convertVectorToBytes(new Float32Array([1.5, 2.5, 3.5]))

  await sql`SELECT
        id,
        document,
        Knn::CosineSimilarity(embedding, ${embedding}) as score
    FROM `table_name`
    ORDER BY score DESC
    LIMIT ${unsafe(limit)};
  ```

- Java

    ```java
    import java.nio.ByteBuffer;
    import java.nio.ByteOrder;
    import java.util.ArrayList;
    import java.util.List;
    import java.util.Optional;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.result.ResultSetReader;
    import tech.ydb.table.values.PrimitiveValue;

    byte[] convertVectorToBytes(float[] vector) {
        ByteBuffer bb = ByteBuffer.allocate(vector.length * Float.BYTES + 1).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            bb.putFloat(v);
        }
        bb.put((byte) 0x01);
        return bb.array();
    }

    List<ResultItem> searchItemsAsBytes(
            SessionRetryContext retryCtx,
            String tableName,
            float[] embedding,
            String strategy,
            long limit,
            Optional<String> indexName) {

        String viewIndex = indexName.map(n -> "VIEW " + n).orElse("");
        String sortOrder = strategy.endsWith("Similarity") ? "DESC" : "ASC";

        String query = String.format("""
                DECLARE $embedding as String;
                SELECT
                    id,
                    document,
                    Knn::%s(embedding, $embedding) as score
                FROM %s %s
                ORDER BY score %s
                LIMIT %d;
                """, strategy, tableName, viewIndex, sortOrder, limit);

        Params params = Params.of("$embedding", PrimitiveValue.newBytes(convertVectorToBytes(embedding)));

        QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(query, TxMode.SERIALIZABLE_RW, params)
        )).join().getValue();

        List<ResultItem> result = new ArrayList<>();
        ResultSetReader rs = reader.getResultSet(0);
        while (rs.next()) {
            result.add(new ResultItem(
                    rs.getColumn("id").getText(),
                    rs.getColumn("document").getText(),
                    rs.getColumn("score").getFloat()
            ));
        }
        return result;
    }

    // record ResultItem(String id, String document, float score) {}
    ```

    Тот же поиск с передачей запросного вектора как `List<Float>`:

    ```java
    import java.util.ArrayList;
    import java.util.List;
    import java.util.Optional;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.result.ResultSetReader;
    import tech.ydb.table.values.ListType;
    import tech.ydb.table.values.ListValue;
    import tech.ydb.table.values.PrimitiveType;
    import tech.ydb.table.values.PrimitiveValue;
    import tech.ydb.table.values.Value;

    List<ResultItem> searchItemsAsFloatList(
            SessionRetryContext retryCtx,
            String tableName,
            float[] embedding,
            String strategy,
            long limit,
            Optional<String> indexName) {

        String viewIndex = indexName.map(n -> "VIEW " + n).orElse("");
        String sortOrder = strategy.endsWith("Similarity") ? "DESC" : "ASC";

        String query = String.format("""
                DECLARE $embedding as List<Float>;

                $target_embedding = Knn::ToBinaryStringFloat($embedding);

                SELECT
                    id,
                    document,
                    Knn::%s(embedding, $target_embedding) as score
                FROM %s %s
                ORDER BY score
                %s
                LIMIT %d;
                """, strategy, tableName, viewIndex, sortOrder, limit);

        Value<?>[] floats = new Value<?>[embedding.length];
        for (int i = 0; i < embedding.length; i++) {
            floats[i] = PrimitiveValue.newFloat(embedding[i]);
        }
        ListValue emb = ListType.of(PrimitiveType.Float).newValueOwn(floats);
        Params params = Params.of("$embedding", emb);

        QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(query, TxMode.SERIALIZABLE_RW, params)
        )).join().getValue();

        List<ResultItem> result = new ArrayList<>();
        ResultSetReader rs = reader.getResultSet(0);
        while (rs.next()) {
            result.add(new ResultItem(
                    rs.getColumn("id").getText(),
                    rs.getColumn("document").getText(),
                    rs.getColumn("score").getFloat()
            ));
        }
        return result;
    }

    // record ResultItem(String id, String document, float score) {}
    ```

- C++ (альтернативный)

    ```cpp
    std::vector<TResultItem> SearchItemsAsFloatList(
        NYdb::NQuery::TQueryClient& client,
        const std::string& tableName,
        const std::vector<float>& embedding,
        const std::string& strategy,
        std::uint64_t limit,
        std::uint64_t topClusters = 10,
        const std::optional<std::string>& indexName = std::nullopt)
    {
        std::string viewIndex = indexName ? "VIEW " + *indexName : "";
        std::string sortOrder = strategy.ends_with("Similarity") ? "DESC" : "ASC";

        std::string query = std::format(R"(
            PRAGMA ydb.KMeansTreeSearchTopSize = "{5}";
            DECLARE $embedding as List<Float>;

            $TargetEmbedding = Knn::ToBinaryStringFloat($embedding);

            SELECT
                id,
                document,
                Knn::{2}(embedding, $TargetEmbedding) as score
            FROM {0} {1}
            ORDER BY score
            {3}
            LIMIT {4};
        )", tableName, viewIndex, strategy, sortOrder, limit, topClusters);

        NYdb::TParamsBuilder paramsBuilder;
        auto& valueBuilder = paramsBuilder.AddParam("$embedding");
        valueBuilder.BeginList();
        for (auto value : embedding) {
            valueBuilder.AddListItem().Float(value);
        }
        valueBuilder.EndList().Build();

        std::vector<TResultItem> result;

        NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([params = paramsBuilder.Build(), &query, &result](NYdb::NQuery::TSession session) {
            auto execResult = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            if (execResult.IsSuccess()) {
                auto parser = execResult.GetResultSetParser(0);
                while (parser.TryNextRow()) {
                    result.push_back({
                        .Id = *parser.ColumnParser(0).GetOptionalUtf8(),
                        .Document = *parser.ColumnParser(1).GetOptionalUtf8(),
                        .Score = *parser.ColumnParser(2).GetOptionalFloat()
                    });
                }
            }
            return execResult;
        }));

        return result;
    }
    ```

- Python (альтернативный)

    {% list tabs %}

    - Native SDK

      ```python
      def search_items_vector_as_float_list(
          pool: ydb.QuerySessionPool,
          table_name: str,
          embedding: list[float],
          strategy: str = "CosineSimilarity",
          limit: int = 1,
          index_name: str | None = None,
          top_clusters: int = 10,
      ) -> list[dict]:
          view_index = f"VIEW {index_name}" if index_name else ""

          sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

          query = f"""
          PRAGMA ydb.KMeansTreeSearchTopSize = "{top_clusters}";
          DECLARE $embedding as List<Float>;

          $target_embedding = Knn::ToBinaryStringFloat($embedding);

          SELECT
              id,
              document,
              Knn::{strategy}(embedding, $target_embedding) as score
          FROM {table_name} {view_index}
          ORDER BY score
          {sort_order}
          LIMIT {limit};
          """

          result = pool.execute_with_retries(
              query,
              {
                  "$embedding": (embedding, ydb.ListType(ydb.PrimitiveType.Float)),
              },
          )

          items = []

          for result_set in result:
              for row in result_set.rows:
                  items.append(
                      {
                          "id": row["id"],
                          "document": row["document"],
                          "score": row["score"],
                      }
                  )

          return items
      ```

    - Native SDK (Asyncio)

      ```python
      import ydb

      async def search_items_vector_as_float_list(
          pool: ydb.aio.QuerySessionPool,
          table_name: str,
          embedding: list[float],
          strategy: str = "CosineSimilarity",
          limit: int = 1,
          index_name: str | None = None,
      ) -> list[dict]:
          view_index = f"VIEW {index_name}" if index_name else ""

          sort_order = "DESC" if strategy.endswith("Similarity") else "ASC"

          query = f"""
          DECLARE $embedding as List<Float>;

          $target_embedding = Knn::ToBinaryStringFloat($embedding);

          SELECT
              id,
              document,
              Knn::{strategy}(embedding, $target_embedding) as score
          FROM {table_name} {view_index}
          ORDER BY score
          {sort_order}
          LIMIT {limit};
          """

          result = await pool.execute_with_retries(
              query,
              {
                  "$embedding": (embedding, ydb.ListType(ydb.PrimitiveType.Float)),
              },
          )

          items = []

          for result_set in result:
              for row in result_set.rows:
                  items.append(
                      {
                          "id": row["id"],
                          "document": row["document"],
                          "score": row["score"],
                      }
                  )

          return items
      ```

    {% endlist %}

- JavaScript (альтернативный)

  ```javascript
  const limit;
  const embedding = new Float32Array([1.5, 2.5, 3.5])

  await sql`SELECT
        id,
        document,
        Knn::CosineSimilarity(embedding, Knn::ToBinaryStringFloat(${embedding})) as score
    FROM `table_name`
    ORDER BY score DESC
    LIMIT ${unsafe(limit)};
  ```

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Итоговый пример {#full-example}

Объединим все вышеописанные методы в один пример, который включает следующие шаги:

1. Удаление существующей таблицы
2. Создание новой таблицы
3. Вставка объектов
4. Поиск ближайших векторов без использования индекса
5. Добавление векторного индекса
6. Поиск ближайших векторов с использованием индекса

{% list tabs %}

- C++

    ```cpp
    void PrintResults(const std::vector<TResultItem>& items)
    {
        if (items.empty()) {
            std::cout << "No items found" << std::endl;
            return;
        }

        for (const auto& item : items) {
            std::cout << "[score=" << item.Score << "] " << item.Id << ": " << item.Document << std::endl;
        }
    }

    void VectorExample(
        const std::string& endpoint,
        const std::string& database,
        const std::string& tableName,
        const std::string& indexName)
    {
        auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
        NYdb::TDriver driver(driverConfig);
        NYdb::NQuery::TQueryClient client(driver);

        try {
            DropVectorTable(client, tableName);
            CreateVectorTable(client, tableName);
            std::vector<TItem> items = {
                {.Id = "1", .Document = "document 1", .Embedding = {0.98, 0.1, 0.01}},
                {.Id = "2", .Document = "document 2", .Embedding = {1.0, 0.05, 0.05}},
                {.Id = "3", .Document = "document 3", .Embedding = {0.9, 0.1, 0.1}},
                {.Id = "4", .Document = "document 4", .Embedding = {0.03, 0.0, 0.99}},
                {.Id = "5", .Document = "document 5", .Embedding = {0.0, 0.0, 0.99}},
                {.Id = "6", .Document = "document 6", .Embedding = {0.0, 0.02, 1.0}},
                {.Id = "7", .Document = "document 7", .Embedding = {0.0, 1.05, 0.05}},
                {.Id = "8", .Document = "document 8", .Embedding = {0.02, 0.98, 0.1}},
                {.Id = "9", .Document = "document 9", .Embedding = {0.0, 1.0, 0.05}},
            };
            InsertItemsAsBytes(client, tableName, items);
            PrintResults(SearchItemsAsBytes(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3));
            AddIndex(driver, client, database, tableName, indexName, "similarity=cosine", 3, 1, 3);
            PrintResults(SearchItemsAsBytes(client, tableName, {1.0, 0.0, 0.0}, "CosineSimilarity", 3, 10, indexName));
        } catch (const std::exception& e) {
            std::cerr << "Execution failed: " << e.what() << std::endl;
        }

        driver.Stop(true);
    }
    ```

    Полный код программы доступен по [ссылке](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/vector_index_builtin).

- Go

    Функциональность векторного поиска полностью поддерживается в Go SDK. Полный пример, объединяющий все вышеописанные операции (создание таблицы, вставка данных, создание индекса, поиск), строится из приведённых выше фрагментов кода. Рабочий пример см. в репозитории [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples).

- Python

    Пример использования

    {% list tabs %}

    - Native SDK

      ```python
      def drop_vector_table_if_exists(pool: ydb.QuerySessionPool, table_name: str) -> None:
          pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table_name}`")

          print("Vector table dropped")

      def main(
          ydb_endpoint: str,
          ydb_database: str,
          ydb_credentials: ydb.AbstractCredentials,
          table_name: str,
          index_name: str,
      ):
          driver = ydb.Driver(
              endpoint=ydb_endpoint,
              database=ydb_database,
              credentials=ydb_credentials,
          )
          driver.wait(5, fail_fast=True)
          pool = ydb.QuerySessionPool(driver)

          drop_vector_table_if_exists(pool, table_name)

          create_vector_table(pool, table_name)

          items = [
              {"id": "1", "document": "vector 1", "embedding": [0.98, 0.1, 0.01]},
              {"id": "2", "document": "vector 2", "embedding": [1.0, 0.05, 0.05]},
              {"id": "3", "document": "vector 3", "embedding": [0.9, 0.1, 0.1]},
              {"id": "4", "document": "vector 4", "embedding": [0.03, 0.0, 0.99]},
              {"id": "5", "document": "vector 5", "embedding": [0.0, 0.0, 0.99]},
              {"id": "6", "document": "vector 6", "embedding": [0.0, 0.02, 1.0]},
              {"id": "7", "document": "vector 7", "embedding": [0.0, 1.05, 0.05]},
              {"id": "8", "document": "vector 8", "embedding": [0.02, 0.98, 0.1]},
              {"id": "9", "document": "vector 9", "embedding": [0.0, 1.0, 0.05]},
          ]

          insert_items_vector_as_bytes(pool, table_name, items)

          items = search_items_vector_as_bytes(
              pool,
              table_name,
              embedding=[1, 0, 0],
              strategy="CosineSimilarity",
              limit=3,
              top_clusters=10,
          )
          print_results(items)

          add_vector_index(
              pool,
              driver,
              table_name,
              index_name=index_name,
              strategy="similarity=cosine",
              dimension=3,
              levels=1,
              clusters=3,
          )

          items = search_items_vector_as_bytes(
              pool,
              table_name,
              embedding=[1, 0, 0],
              index_name=index_name,
              strategy="CosineSimilarity",
              limit=3,
              top_clusters=10,
          )
          print_results(items)

        def print_results(items):
            if len(items) == 0:
                print("No items found")
                return

            for item in items:
                print(f"[score={item['score']}] {item['id']}: {item['document']}")
      ```

    - Native SDK (asyncio)

      ```python
        async def drop_vector_table_if_exists(pool: ydb.aio.QuerySessionPool, table_name: str) -> None:
            await pool.execute_with_retries(f"DROP TABLE IF EXISTS `{table_name}`")

            print("Vector table dropped")

        async def main(
            ydb_endpoint: str,
            ydb_database: str,
            ydb_credentials: ydb.AbstractCredentials,
            table_name: str,
            index_name: str,
        ):
            async with ydb.aio.Driver(
                endpoint=ydb_endpoint,
                database=ydb_database,
                credentials=ydb_credentials,
            ) as driver:
                await driver.wait(5, fail_fast=True)
                pool = ydb.aio.QuerySessionPool(driver)

                await drop_vector_table_if_exists(pool, table_name)

                await create_vector_table(pool, table_name)

                items = [
                    {"id": "1", "document": "vector 1", "embedding": [0.98, 0.1, 0.01]},
                    {"id": "2", "document": "vector 2", "embedding": [1.0, 0.05, 0.05]},
                    {"id": "3", "document": "vector 3", "embedding": [0.9, 0.1, 0.1]},
                    {"id": "4", "document": "vector 4", "embedding": [0.03, 0.0, 0.99]},
                    {"id": "5", "document": "vector 5", "embedding": [0.0, 0.0, 0.99]},
                    {"id": "6", "document": "vector 6", "embedding": [0.0, 0.02, 1.0]},
                    {"id": "7", "document": "vector 7", "embedding": [0.0, 1.05, 0.05]},
                    {"id": "8", "document": "vector 8", "embedding": [0.02, 0.98, 0.1]},
                    {"id": "9", "document": "vector 9", "embedding": [0.0, 1.0, 0.05]},
                ]

                await insert_items_vector_as_bytes(pool, table_name, items)

                items = await search_items_vector_as_bytes(
                    pool,
                    table_name,
                    embedding=[1, 0, 0],
                    strategy="CosineSimilarity",
                    limit=3,
                )
                print_results(items)

                await add_vector_index(
                    pool,
                    driver,
                    table_name,
                    index_name=index_name,
                    strategy="similarity=cosine",
                    dimension=3,
                    levels=1,
                    clusters=3,
                )

                items = await search_items_vector_as_bytes(
                    pool,
                    table_name,
                    embedding=[1, 0, 0],
                    index_name=index_name,
                    strategy="CosineSimilarity",
                    limit=3,
                )
                print_results(items)

                await pool.stop()

        if __name__ == "__main__":
            asyncio.run(main(
                ydb_endpoint=os.environ.get("YDB_ENDPOINT", "grpc://localhost:2136"),
                ydb_database=os.environ.get("YDB_DATABASE", "/local"),
                ydb_credentials=ydb.credentials_from_env_variables(),
                table_name="ydb_vector_search",
                index_name="ydb_vector_index",
            ))
      ```

    {% endlist %}

    Вывод программы:

    ```bash
    Vector table dropped
    Vector table created
    9 items inserted
    [score=0.997509241104126] 2: vector 2
    [score=0.9947828650474548] 1: vector 1
    [score=0.9878783822059631] 3: vector 3
    Table index ydb_vector_index created.
    [score=0.997509241104126] 2: vector 2
    [score=0.9947828650474548] 1: vector 1
    [score=0.9878783822059631] 3: vector 3
    ```

    В результате видно, что таблица была создана, добавлено 9 документов и успешно выполнен поиск по близости векторов — как до, так и после добавления векторного индекса.

    Полный код программы доступен по [ссылке](https://github.com/ydb-platform/ydb/blob/main/ydb/public/sdk/python/examples/vector_search/vector_search.py).

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

    Пример объединяет шаги из разделов выше: `QueryClient` + `SessionRetryContext` для YQL и `TableClient` + `SessionRetryContext` для `ALTER TABLE` с переименованием индекса. Методы `createVectorTable`, `insertItemsAsBytes`, `searchItemsAsBytes`, `addVectorIndex` и тип `Item` / `ResultItem` — как в соответствующих фрагментах этой страницы.

    ```java
    import java.util.List;
    import java.util.Optional;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.TableClient;
    import tech.ydb.table.query.Params;

    public class VectorSearchJavaExample {

        record Item(String id, String document, float[] embedding) {}
        record ResultItem(String id, String document, float score) {}

        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault("YDB_CONNECTION_STRING", "grpc://localhost:2136/local");
            String tableName = "ydb_vector_search";
            String indexName = "ydb_vector_index";

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport).build();
                 TableClient tableClient = TableClient.newClient(transport).build()) {

                SessionRetryContext queryRetry = SessionRetryContext.create(queryClient).build();
                SessionRetryContext tableRetry = SessionRetryContext.create(tableClient).build();

                dropVectorTableIfExists(queryRetry, tableName);
                createVectorTable(queryRetry, tableName);

                List<Item> items = List.of(
                        new Item("1", "vector 1", new float[]{0.98f, 0.1f, 0.01f}),
                        new Item("2", "vector 2", new float[]{1.0f, 0.05f, 0.05f}),
                        new Item("3", "vector 3", new float[]{0.9f, 0.1f, 0.1f}),
                        new Item("4", "vector 4", new float[]{0.03f, 0.0f, 0.99f}),
                        new Item("5", "vector 5", new float[]{0.0f, 0.0f, 0.99f}),
                        new Item("6", "vector 6", new float[]{0.0f, 0.02f, 1.0f}),
                        new Item("7", "vector 7", new float[]{0.0f, 1.05f, 0.05f}),
                        new Item("8", "vector 8", new float[]{0.02f, 0.98f, 0.1f}),
                        new Item("9", "vector 9", new float[]{0.0f, 1.0f, 0.05f})
                );

                insertItemsAsBytes(queryRetry, tableName, items);
                printResults(searchItemsAsBytes(queryRetry, tableName, new float[]{1, 0, 0},
                        "CosineSimilarity", 3, Optional.empty()));

                addVectorIndex(transport, queryRetry, tableRetry, tableName, indexName,
                        "similarity=cosine", 3, 1, 3);

                printResults(searchItemsAsBytes(queryRetry, tableName, new float[]{1, 0, 0},
                        "CosineSimilarity", 3, Optional.of(indexName)));
            }
        }

        static void dropVectorTableIfExists(SessionRetryContext queryRetry, String tableName) {
            String ddl = String.format("DROP TABLE IF EXISTS `%s`", tableName);
            queryRetry.supplyResult(s -> QueryReader.readFrom(
                    s.createQuery(ddl, TxMode.NONE, Params.empty())
            )).join().getValue();
            System.out.println("Vector table dropped");
        }

        static void printResults(List<ResultItem> items) {
            if (items.isEmpty()) {
                System.out.println("No items found");
                return;
            }
            for (ResultItem item : items) {
                System.out.printf("[score=%f] %s: %s%n", item.score(), item.id(), item.document());
            }
        }
    }
    ```

    Вывод совпадает с примером на Python.

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

# Эффективная передача эмбеддингов в kNN-поиске {{ ydb-short-name }}

## Проблема

При использовании kNN (k-Nearest Neighbors) поиска в {{ ydb-short-name }} для векторного поиска пользователи часто передают эмбеддинги как `List<Float>`, что приводит к значительным накладным расходам на сериализацию и десериализацию данных.

**Плохой подход** - передача эмбеддинга как списка чисел с плавающей точкой:

```yql
DECLARE $embedding AS List<Float>;

$query_vector = Knn::ToBinaryStringFloat($embedding);

SELECT url, Knn::CosineSimilarity(embedding,$query_vector) AS score
FROM embeddings
ORDER BY score DESC
LIMIT 100;
```

Этот подход требует:
- Сериализации массива float в {{ ydb-short-name }} SDK
- Десериализации в {{ ydb-short-name }}
- Преобразования в бинарный формат функцией `Knn::ToBinaryStringFloat()`

Для эмбеддингов размером 512-1024 измерений это создает значительные накладные расходы.

## Решение

Передавать эмбеддинги сразу в бинарном формате как `Bytes`, минуя промежуточные преобразования.

**Хороший подход** - передача предварительно сериализованного бинарного вектора:

```yql
DECLARE $embedding AS Bytes;

SELECT url, Knn::CosineSimilarity(embedding,$embedding) AS score
FROM embeddings
ORDER BY score DESC
LIMIT 100;
```

Этот подход:
- Устраняет накладные расходы на сериализацию/десериализацию
- Позволяет использовать предварительно подготовленные бинарные векторы
- Ускоряет выполнение запроса на 30-50%


{% list tabs %}

- Go

  {% cut "Плохой пример" %}
  ```go
// Плохой пример: передача эмбеддинга как списка float
func searchEmbeddingsBad(db *ydb.Driver, embedding []float32) ([]SearchResult, error) {
    query := `
        DECLARE $embedding AS List<Float>;
        
        $query_vector = Knn::ToBinaryStringFloat($embedding);
        
        SELECT url, Knn::CosineSimilarity(embedding,$query_vector) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    `
    
    params := table.NewQueryParameters(
        table.ValueParam("$embedding", types.ListValue(types.FloatValue(embedding...))),
    )
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```go
// Хороший пример: передача предварительно сериализованного бинарного вектора
func searchEmbeddingsGood(db *ydb.Driver, embedding []float32) ([]SearchResult, error) {
    query := `
        DECLARE $embedding AS Bytes;
        
        SELECT url, Knn::CosineSimilarity(embedding,$embedding) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    `
    
    // Сериализация float32 в бинарный формат (little-endian + байт типа в конце)
    // Формат соответствует Knn::ToBinaryStringFloat(): данные + 1 байт типа (0x01 = Float)
    binaryVector := make([]byte, len(embedding)*4+1)
    for i, v := range embedding {
        binary.LittleEndian.PutUint32(binaryVector[i*4:], math.Float32bits(v))
    }
    binaryVector[len(embedding)*4] = 0x01 // тип Float
    
    params := table.NewQueryParameters(
        table.ValueParam("$embedding", types.BytesValue(binaryVector)),
    )
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

- Python

  {% cut "Плохой пример" %}
  ```python
# Плохой пример: передача эмбеддинга как списка float
def search_embeddings_bad(session, embedding):
    query = """
        DECLARE $embedding AS List<Float>;
        
        $query_vector = Knn::ToBinaryStringFloat($embedding);
        
        SELECT url, Knn::CosineSimilarity(embedding,$query_vector) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    """
    
    params = {
        '$embedding': embedding  # список float
    }
    
    # Выполнение запроса...
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```python
# Хороший пример: передача предварительно сериализованного бинарного вектора
import struct

def search_embeddings_good(session, embedding):
    query = """
        DECLARE $embedding AS Bytes;
        
        SELECT url, Knn::CosineSimilarity(embedding,$embedding) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    """
    
    # Сериализация float32 в бинарный формат (little-endian + байт типа в конце)
    # Формат: данные + 1 байт типа (0x01 = Float)
    binary_vector = b''.join(struct.pack('<f', x) for x in embedding) + b'\x01'
    
    params = {
        '$embedding': binary_vector
    }
    
    # Выполнение запроса...
  ```
  {% endcut %}

- Java

  {% cut "Плохой пример" %}
  ```java
// Плохой пример: передача эмбеддинга как списка float
public List<SearchResult> searchEmbeddingsBad(Session session, float[] embedding) {
    String query = """
        DECLARE $embedding AS List<Float>;
        
        $query_vector = Knn::ToBinaryStringFloat($embedding);
        
        SELECT url, Knn::CosineSimilarity(embedding,$query_vector) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    """;
    
    Params params = Params.of(
        "$embedding", PrimitiveValue.newFloatList(embedding)
    );
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```java
// Хороший пример: передача предварительно сериализованного бинарного вектора
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public List<SearchResult> searchEmbeddingsGood(Session session, float[] embedding) {
    String query = """
        DECLARE $embedding AS Bytes;
        
        SELECT url, Knn::CosineSimilarity(embedding,$embedding) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    """;
    
    // Сериализация float в бинарный формат (little-endian + байт типа в конце)
    // Формат соответствует Knn::ToBinaryStringFloat(): данные + 1 байт типа (0x01 = Float)
    ByteBuffer buffer = ByteBuffer.allocate(embedding.length * 4 + 1);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    for (float f : embedding) {
        buffer.putFloat(f);
    }
    buffer.put((byte) 0x01); // тип Float
    
    Params params = Params.of(
        "$embedding", PrimitiveValue.newBytes(buffer.array())
    );
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

- JavaScript

  {% cut "Плохой пример" %}
  ```javascript
// Плохой пример: передача эмбеддинга как списка float
async function searchEmbeddingsBad(session, embedding) {
    const query = `
        DECLARE $embedding AS List<Float>;
        
        $query_vector = Knn::ToBinaryStringFloat($embedding);
        
        SELECT url, Knn::CosineSimilarity(embedding,$query_vector) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    `;
    
    const params = {
        '$embedding': { items: embedding.map(f => ({ floatValue: f })) }
    };
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

  {% cut "Хороший пример" %}
  ```javascript
// Хороший пример: передача предварительно сериализованного бинарного вектора
async function searchEmbeddingsGood(session, embedding) {
    const query = `
        DECLARE $embedding AS Bytes;
        
        SELECT url, Knn::CosineSimilarity(embedding,$embedding) AS score
        FROM embeddings
        ORDER BY score DESC
        LIMIT 100;
    `;
    
    // Сериализация float32 в бинарный формат (little-endian + байт типа в конце)
    // Формат соответствует Knn::ToBinaryStringFloat(): данные + 1 байт типа (0x01 = Float)
    const buffer = new ArrayBuffer(embedding.length * 4 + 1);
    const view = new DataView(buffer);
    embedding.forEach((value, index) => {
        view.setFloat32(index * 4, value, true); // true для little-endian
    });
    view.setUint8(embedding.length * 4, 0x01); // тип Float
    
    const params = {
        '$embedding': { bytesValue: new Uint8Array(buffer) }
    };
    
    // Выполнение запроса...
}
  ```
  {% endcut %}

{% endlist %}


## Рекомендации

1. **Используйте бинарный формат** для передачи эмбеддингов в kNN-запросах
2. **Предварительно сериализуйте** векторы на стороне приложения
3. **Проверяйте порядок байт** (little-endian) при сериализации
4. **Кэшируйте сериализованные векторы** для повторного использования
5. **Тестируйте производительность** обоих подходов на ваших данных

## Когда использовать

**Используйте бинарный формат когда:**
- Работаете с большими эмбеддингами (512+ измерений)
- Выполняете частые kNN-запросы
- Требуется максимальная производительность

**Можно использовать List<Float> когда:**
- Работаете с небольшими векторами (<100 измерений)
- Производительность не критична
- Упрощение кода важнее производительности

Для более подробной информации о сериализации эмбеддингов в разных языках программирования обратитесь к документации: https://ydb.yandex-team.ru/docs/recipes/ydb-sdk/vector-search

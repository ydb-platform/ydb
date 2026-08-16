# LangChain

Интеграция {{ ydb-short-name }} с [LangChain](https://python.langchain.com/docs/introduction/) позволяет использовать {{ ydb-short-name }} в качестве [векторного хранилища](https://python.langchain.com/docs/concepts/vectorstores/) для [RAG](https://python.langchain.com/docs/concepts/rag/) приложений.

Эта интеграция позволяет разработчикам эффективно управлять, запрашивать и извлекать векторизованные данные, что является основой для современных приложений, связанных с обработкой естественного языка, поиском и анализом данных. Используя модели эмбеддингов, пользователи могут создавать сложные системы, способные понимать и извлекать информацию на основе семантического сходства.

Интеграция доступна для Python и JavaScript.

## Установка {#setup}

Для использования этой интеграции установите локальный {{ ydb-short-name }}. Для получения дополнительной информации см. [{#T}](../../quickstart.md#install).

Также установите пакеты LangChain и модель эмбеддингов для нужного языка:

{% list tabs group=lang %}

- Python

  ```shell
  pip install -qU langchain-ydb
  pip install -qU langchain-huggingface
  ```

- JavaScript

  ```shell
  npm install @ydbjs/langchain @langchain/core
  npm install @langchain/community @huggingface/transformers
  ```

{% endlist %}

## Инициализация {#initialization}

Для создания векторного хранилища {{ ydb-short-name }} требуется указать модель эмбеддингов и параметры подключения:

{% list tabs group=lang %}

- Python

  ```python
  from langchain_huggingface import HuggingFaceEmbeddings
  from langchain_ydb.vectorstores import YDB, YDBSearchStrategy, YDBSettings

  embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")

  settings = YDBSettings(
      host="localhost",
      port=2136,
      database="/local",
      table="ydb_example",
      strategy=YDBSearchStrategy.COSINE_SIMILARITY,
  )
  vector_store = YDB(embeddings, config=settings)
  ```

- JavaScript

  ```javascript
  import { HuggingFaceTransformersEmbeddings } from "@langchain/community/embeddings/huggingface_transformers";
  import { YDBSearchStrategy, YDBVectorStore } from "@ydbjs/langchain";

  const embeddings = new HuggingFaceTransformersEmbeddings({
    model: "sentence-transformers/all-mpnet-base-v2",
  });

  const vectorStore = new YDBVectorStore(embeddings, {
    connectionString: "grpc://localhost:2136/local",
    table: "ydb_example",
    strategy: YDBSearchStrategy.CosineSimilarity,
  });
  ```

{% endlist %}

## Управление векторным хранилищем {#manage_vector_store}

Создав векторное хранилище, можно взаимодействовать с ним, добавляя и удаляя различные элементы.

### Добавление элементов {#add_items_to_vector_store}

Подготовьте документы для работы:

{% list tabs group=lang %}

- Python

  ```python
  from uuid import uuid4

  from langchain_core.documents import Document

  uuids = [str(uuid4()) for _ in range(10)]
  documents = [
      Document(
          page_content="I had chocolate chip pancakes and scrambled eggs for breakfast this morning.",
          metadata={"source": "tweet"},
          id=uuids[0],
      ),
      Document(
          page_content="The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees.",
          metadata={"source": "news"},
          id=uuids[1],
      ),
      Document(
          page_content="Building an exciting new project with LangChain - come check it out!",
          metadata={"source": "tweet"},
          id=uuids[2],
      ),
      Document(
          page_content="Robbers broke into the city bank and stole $1 million in cash.",
          metadata={"source": "news"},
          id=uuids[3],
      ),
      Document(
          page_content="Wow! That was an amazing movie. I can't wait to see it again.",
          metadata={"source": "tweet"},
          id=uuids[4],
      ),
      Document(
          page_content="Is the new iPhone worth the price? Read this review to find out.",
          metadata={"source": "website"},
          id=uuids[5],
      ),
      Document(
          page_content="The top 10 soccer players in the world right now.",
          metadata={"source": "website"},
          id=uuids[6],
      ),
      Document(
          page_content="LangGraph is the best framework for building stateful, agentic applications!",
          metadata={"source": "tweet"},
          id=uuids[7],
      ),
      Document(
          page_content="The stock market is down 500 points today due to fears of a recession.",
          metadata={"source": "news"},
          id=uuids[8],
      ),
      Document(
          page_content="I have a bad feeling I am going to get deleted :(",
          metadata={"source": "tweet"},
          id=uuids[9],
      ),
  ]
  ```

  Добавьте документы в векторное хранилище:

  ```python
  ids = vector_store.add_documents(documents=documents)
  ```

- JavaScript

  ```javascript
  import { Document } from "@langchain/core/documents";
  import { randomUUID } from "node:crypto";

  const uuids = Array.from({ length: 10 }, () => randomUUID());
  const documents = [
    new Document({
      pageContent: "I had chocolate chip pancakes and scrambled eggs for breakfast this morning.",
      metadata: { source: "tweet" },
      id: uuids[0],
    }),
    new Document({
      pageContent: "The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees.",
      metadata: { source: "news" },
      id: uuids[1],
    }),
    new Document({
      pageContent: "Building an exciting new project with LangChain - come check it out!",
      metadata: { source: "tweet" },
      id: uuids[2],
    }),
    new Document({
      pageContent: "Robbers broke into the city bank and stole $1 million in cash.",
      metadata: { source: "news" },
      id: uuids[3],
    }),
    new Document({
      pageContent: "Wow! That was an amazing movie. I can't wait to see it again.",
      metadata: { source: "tweet" },
      id: uuids[4],
    }),
    new Document({
      pageContent: "Is the new iPhone worth the price? Read this review to find out.",
      metadata: { source: "website" },
      id: uuids[5],
    }),
    new Document({
      pageContent: "The top 10 soccer players in the world right now.",
      metadata: { source: "website" },
      id: uuids[6],
    }),
    new Document({
      pageContent: "LangGraph is the best framework for building stateful, agentic applications!",
      metadata: { source: "tweet" },
      id: uuids[7],
    }),
    new Document({
      pageContent: "The stock market is down 500 points today due to fears of a recession.",
      metadata: { source: "news" },
      id: uuids[8],
    }),
    new Document({
      pageContent: "I have a bad feeling I am going to get deleted :(",
      metadata: { source: "tweet" },
      id: uuids[9],
    }),
  ];
  ```

  Добавьте документы в векторное хранилище:

  ```javascript
  const ids = await vectorStore.addDocuments(documents);
  ```

{% endlist %}

### Удаление элементов {#delete_items_from_vector_store}

Элементы удаляются из векторного хранилища по идентификатору с использованием функции `delete`:

{% list tabs group=lang %}

- Python

  ```python
  vector_store.delete(ids=[ids[-1]])
  ```

- JavaScript

  ```javascript
  await vectorStore.delete({ ids: [ids.at(-1)] });
  ```

{% endlist %}

## Запросы в векторное хранилище {#query_vector_store}

После создания векторного хранилища и добавления в него необходимых документов появляется возможность выполнения поисковых запросов в процессе выполнения цепочки или агента.

### Прямой запрос {#query_directly}

#### Поиск по сходству

Простой поиск по сходству можно выполнить следующим образом:

{% list tabs group=lang %}

- Python

  ```python
  results = vector_store.similarity_search(
      "LangChain provides abstractions to make working with LLMs easy",
      k=2,
  )
  for res in results:
      print(f"* {res.page_content} [{res.metadata}]")
  ```

- JavaScript

  ```javascript
  const results = await vectorStore.similaritySearch(
    "LangChain provides abstractions to make working with LLMs easy",
    2,
  );
  for (const res of results) {
    console.log(`* ${res.pageContent} [${JSON.stringify(res.metadata)}]`);
  }
  ```

{% endlist %}

Результат:

```shell
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```

#### Поиск по сходству с оценкой

Также можно выполнить поиск с оценкой:

{% list tabs group=lang %}

- Python

  ```python
  results = vector_store.similarity_search_with_score(
      "Will it be hot tomorrow?",
      k=3,
  )
  for res, score in results:
      print(f"* [SIM={score:.3f}] {res.page_content} [{res.metadata}]")
  ```

- JavaScript

  ```javascript
  const results = await vectorStore.similaritySearchWithScore(
    "Will it be hot tomorrow?",
    3,
  );
  for (const [res, score] of results) {
    console.log(`* [SIM=${score.toFixed(3)}] ${res.pageContent} [${JSON.stringify(res.metadata)}]`);
  }
  ```

{% endlist %}

Результат:

```shell
* [SIM=0.595] The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees. [{'source': 'news'}]
* [SIM=0.212] I had chocolate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* [SIM=0.118] Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
```

### Фильтрация {#filtering}

Поиск с использованием фильтров выполняется следующим образом:

{% list tabs group=lang %}

- Python

  ```python
  results = vector_store.similarity_search_with_score(
      "What did I eat for breakfast?",
      k=4,
      filter={"source": "tweet"},
  )
  for res, _ in results:
      print(f"* {res.page_content} [{res.metadata}]")
  ```

- JavaScript

  ```javascript
  const results = await vectorStore.similaritySearchWithScore(
    "What did I eat for breakfast?",
    4,
    { source: "tweet" },
  );
  for (const [res] of results) {
    console.log(`* ${res.pageContent} [${JSON.stringify(res.metadata)}]`);
  }
  ```

{% endlist %}

Результат:

```shell
* I had chocolate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```

### Запрос через трансформацию в ретривер {#query_by_turning_into_retriever}

Векторное хранилище можно трансформировать в поисковик (retriever) для упрощённого использования в цепочках.

Пример представлен ниже:

{% list tabs group=lang %}

- Python

  ```python
  retriever = vector_store.as_retriever(
      search_kwargs={
          "k": 2,
          "filter": {"source": "news"},
      },
  )
  results = retriever.invoke("Stealing from the bank is a crime")
  for res in results:
      print(f"* {res.page_content} [{res.metadata}]")
  ```

- JavaScript

  ```javascript
  const retriever = vectorStore.asRetriever({
    k: 2,
    filter: { source: "news" },
  });
  const results = await retriever.invoke("Stealing from the bank is a crime");
  for (const res of results) {
    console.log(`* ${res.pageContent} [${JSON.stringify(res.metadata)}]`);
  }
  ```

{% endlist %}

Результат:

```shell
* Robbers broke into the city bank and stole $1 million in cash. [{'source': 'news'}]
* The stock market is down 500 points today due to fears of a recession. [{'source': 'news'}]
```

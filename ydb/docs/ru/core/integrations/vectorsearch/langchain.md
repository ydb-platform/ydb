# LangChain

Интеграция {{ ydb-short-name }} с [langchain](https://python.langchain.com/docs/introduction/) позволяет использовать {{ ydb-short-name }} в качестве [векторного хранилища](https://python.langchain.com/docs/concepts/vectorstores/) для [RAG](https://python.langchain.com/docs/concepts/rag/) приложений.

Эта интеграция позволяет разработчикам эффективно управлять, запрашивать и извлекать векторизованные данные, что является основой для современных приложений, связанных с обработкой естественного языка, поиском и анализом данных. Используя модели эмбеддингов, пользователи могут создавать сложные системы, способные понимать и извлекать информацию на основе семантического сходства.

## Установка {#setup}

Для использования этой интеграции установите следующее программное обеспечение:

- `langchain-ydb`

    Для установки `langchain-ydb`, полните следующую команду:

    ```shell
    pip install -qU langchain-ydb
    ```

- Модель ембеддингов

    В этом руководстве используется `HuggingFaceEmbeddings`. Чтобы установить этот пакет, выполните следующую команду:

    ```shell
    pip install -qU langchain-huggingface
    ```

- Локальный {{ ydb-short-name }}

    Для получения дополнительной информации см. [{#T}](../../quickstart.md#install).

## Инициализация {#initialization}

Для создания векторного хранилища {{ ydb-short-name }}, требуется указать модель эмбеддингов. В данном примере используется `HuggingFaceEmbeddings`:

```python
from langchain_huggingface import HuggingFaceEmbeddings

embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
```

После создания модели эмбеддингов создаётся векторное хранилище {{ ydb-short-name }}:

```python
from langchain_ydb.vectorstores import YDB, YDBSearchStrategy, YDBSettings

settings = YDBSettings(
    host="localhost",
    port=2136,
    database="/local",
    table="ydb_example",
    strategy=YDBSearchStrategy.COSINE_SIMILARITY,
)
vector_store = YDB(embeddings, config=settings)
```

## Управление векторным хранилищем {#manage_vector_store}

Создав векторное хранилище, можно взаимодействовать с ним, добавляя и удаляя различные элементы.

### Добавление элементов {#add_items_to_vector_store}

Подготавливаются документы для работы:

```python
from uuid import uuid4

from langchain_core.documents import Document

document_1 = Document(
    page_content="I had chocalate chip pancakes and scrambled eggs for breakfast this morning.",
    metadata={"source": "tweet"},
)

document_2 = Document(
    page_content="The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees.",
    metadata={"source": "news"},
)

document_3 = Document(
    page_content="Building an exciting new project with LangChain - come check it out!",
    metadata={"source": "tweet"},
)

document_4 = Document(
    page_content="Robbers broke into the city bank and stole $1 million in cash.",
    metadata={"source": "news"},
)

document_5 = Document(
    page_content="Wow! That was an amazing movie. I can't wait to see it again.",
    metadata={"source": "tweet"},
)

document_6 = Document(
    page_content="Is the new iPhone worth the price? Read this review to find out.",
    metadata={"source": "website"},
)

document_7 = Document(
    page_content="The top 10 soccer players in the world right now.",
    metadata={"source": "website"},
)

document_8 = Document(
    page_content="LangGraph is the best framework for building stateful, agentic applications!",
    metadata={"source": "tweet"},
)

document_9 = Document(
    page_content="The stock market is down 500 points today due to fears of a recession.",
    metadata={"source": "news"},
)

document_10 = Document(
    page_content="I have a bad feeling I am going to get deleted :(",
    metadata={"source": "tweet"},
)

documents = [
    document_1,
    document_2,
    document_3,
    document_4,
    document_5,
    document_6,
    document_7,
    document_8,
    document_9,
    document_10,
]
uuids = [str(uuid4()) for _ in range(len(documents))]
```

Элементы добавляются в векторное хранилище с помощью функции `add_documents`.

```python
vector_store.add_documents(documents=documents, ids=uuids)
```

Результат:

```shell
Inserting data...: 100%|██████████| 10/10 [00:00<00:00, 14.67it/s]
['947be6aa-d489-44c5-910e-62e4d58d2ffb',
 '7a62904d-9db3-412b-83b6-f01b34dd7de3',
 'e5a49c64-c985-4ed7-ac58-5ffa31ade699',
 '99cf4104-36ab-4bd5-b0da-e210d260e512',
 '5810bcd0-b46e-443e-a663-e888c9e028d1',
 '190c193d-844e-4dbb-9a4b-b8f5f16cfae6',
 'f8912944-f80a-4178-954e-4595bf59e341',
 '34fc7b09-6000-42c9-95f7-7d49f430b904',
 '0f6b6783-f300-4a4d-bb04-8025c4dfd409',
 '46c37ba9-7cf2-4ac8-9bd1-d84e2cb1155c']
```

### Удаление элементов {#delete_items_from_vector_store}

Элементы удаляются из векторного хранилища по идентификатору с использованием функции `delete`.

```python
vector_store.delete(ids=[uuids[-1]])
```

Результат:

```shell
True
```

## Запросы в векторное хранилище {#query_vector_store}

После создания векторного хранилища и добавления в него необходимых документов появляется возможность выполнения поисковых запросов в процессе выполнения цепочки или агента.

### Прямой запрос {#query_directly}

#### Поиск по сходству

Простой поиск по сходству можно выполнить следующим образом:

```python
results = vector_store.similarity_search(
    "LangChain provides abstractions to make working with LLMs easy", k=2
)
for res in results:
    print(f"* {res.page_content} [{res.metadata}]")
```

Результат:

```shell
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```

#### Поиск по сходству с оценкой

Также можно выполнить поиск с оценкой:

```python
results = vector_store.similarity_search_with_score("Will it be hot tomorrow?", k=3)
for res, score in results:
    print(f"* [SIM={score:.3f}] {res.page_content} [{res.metadata}]")
```

Результат:

```shell
* [SIM=0.595] The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees. [{'source': 'news'}]
* [SIM=0.212] I had chocalate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* [SIM=0.118] Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
```

### Фильтрация {#filtering}

Поиск с использованием фильтров выполняется следующим образом:

```python
results = vector_store.similarity_search_with_score(
    "What did I eat for breakfast?",
    k=4,
    filter={"source": "tweet"},
)
for res, _ in results:
    print(f"* {res.page_content} [{res.metadata}]")
```

Результат:

```shell
* I had chocalate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```


### Запрос через трансформацию в ретривер {#query_by_turning_into_retriever}

Векторное хранилище можно трансформировать в поисковик (retriever) для упрощённого использования в цепочках.

Пример представлен ниже:

```python
retriever = vector_store.as_retriever(
    search_kwargs={"k": 2},
)
results = retriever.invoke(
    "Stealing from the bank is a crime", filter={"source": "news"}
)
for res in results:
    print(f"* {res.page_content} [{res.metadata}]")
```

Результат:

```shell
* Robbers broke into the city bank and stole $1 million in cash. [{'source': 'news'}]
* The stock market is down 500 points today due to fears of a recession. [{'source': 'news'}]
```

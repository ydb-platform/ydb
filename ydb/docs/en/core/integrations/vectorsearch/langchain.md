# LangChain

Integration of {{ ydb-short-name }} with [langchain](https://python.langchain.com/docs/introduction/) enables the use of {{ ydb-short-name }} as a [vector store](https://python.langchain.com/docs/concepts/vectorstores/) for [RAG](https://python.langchain.com/docs/concepts/rag/) applications.

This integration allows developers to efficiently manage, query, and retrieve vectorized data, which is fundamental for modern applications involving natural language processing, search, and data analysis. By leveraging embedding models, users can create sophisticated systems that understand and retrieve information based on semantic similarity.

## Setup {#setup}

To use this integration, install the following software:

- `langchain-ydb`

    To install `langchain-ydb`, run the following command:

    ```shell
    pip install -qU langchain-ydb
    ```

- embedding model

    This tutorial uses `HuggingFaceEmbeddings`. To install this package, run the following command:

    ```shell
    pip install -qU langchain-huggingface
    ```

- Local {{ ydb-short-name }}

    For more information, see [{#T}](../../quickstart.md#install).

## Initialization {#initialization}

Creating a {{ ydb-short-name }} vector store requires specifying an embedding model. In this instance, `HuggingFaceEmbeddings` is used:

```python
from langchain_huggingface import HuggingFaceEmbeddings

embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-mpnet-base-v2")
```

Once the embedding model is created, the {{ ydb-short-name }} vector store can be initiated:

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

## Manage Vector Store {#manage_vector_store}

After the vector store has been established, you can start adding and removing items from the store.

### Add items to vector store {#add_items_to_vector_store}

The following code prepares the documents:

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

Items are added to the vector store using the `add_documents` function.

```python
vector_store.add_documents(documents=documents, ids=uuids)
```

Output:

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

### Delete items from vector store {#delete_items_from_vector_store}

To delete items from the vector store by ID, use the `delete` function:

```python
vector_store.delete(ids=[uuids[-1]])
```

Output:

```shell
True
```

## Query Vector Store {#query_vector_store}

After establishing the vector store and adding relevant documents, you can query the store during chain or agent execution.

### Query directly {#query_directly}

#### Similarity search

A simple similarity search can be performed as follows:

```python
results = vector_store.similarity_search(
    "LangChain provides abstractions to make working with LLMs easy", k=2
)
for res in results:
    print(f"* {res.page_content} [{res.metadata}]")
```

Output:

```shell
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```

#### Similarity search with score

To perform a similarity search with score, use the following code:

```python
results = vector_store.similarity_search_with_score("Will it be hot tomorrow?", k=3)
for res, score in results:
    print(f"* [SIM={score:.3f}] {res.page_content} [{res.metadata}]")
```

Output:

```shell
* [SIM=0.595] The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees. [{'source': 'news'}]
* [SIM=0.212] I had chocalate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* [SIM=0.118] Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
```

### Filtering {#filtering}

Searching with filters is performed as described below:

```python
results = vector_store.similarity_search_with_score(
    "What did I eat for breakfast?",
    k=4,
    filter={"source": "tweet"},
)
for res, _ in results:
    print(f"* {res.page_content} [{res.metadata}]")
```

Output:

```shell
* I had chocalate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```


### Query by turning into retriever {#query_by_turning_into_retriever}

The vector store can also be transformed into a retriever for easier use in chains.

Here's how to transform the vector store into a retriever and invoke it with a simple query and filter.

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

Output:

```shell
* Robbers broke into the city bank and stole $1 million in cash. [{'source': 'news'}]
* The stock market is down 500 points today due to fears of a recession. [{'source': 'news'}]
```

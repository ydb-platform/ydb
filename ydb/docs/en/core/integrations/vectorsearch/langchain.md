# LangChain

Integration of {{ ydb-short-name }} with [LangChain](https://python.langchain.com/docs/introduction/) allows using {{ ydb-short-name }} as a [vector store](https://python.langchain.com/docs/concepts/vectorstores/) for [RAG](https://python.langchain.com/docs/concepts/rag/) applications.

This integration allows developers to efficiently manage, query, and retrieve vectorized data, which is the foundation for modern applications related to natural language processing, search, and data analysis. Using embedding models, users can create sophisticated systems capable of understanding and retrieving information based on semantic similarity.

The integration is available for Python and JavaScript.

## Installation {#setup}

To use this integration, install a local {{ ydb-short-name }}. For more information, see [{#T}](../../quickstart.md#install).

Also install the LangChain packages and an embedding model for the required language:

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

## Initialization {#initialization}

To create a vector store {{ ydb-short-name }}, you need to specify an embedding model and connection parameters:

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

## Managing the Vector Store {#manage_vector_store}

Once you have created a vector store, you can interact with it by adding and removing various items.

### Adding Items {#add_items_to_vector_store}

Prepare the documents for processing:

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


  Add the documents to the vector store:


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


  Add the documents to the vector store:


  ```javascript
  const ids = await vectorStore.addDocuments(documents);
  ```

{% endlist %}

### Deleting Items {#delete_items_from_vector_store}

Items are deleted from the vector store by ID using the `delete` function:

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

## Querying the Vector Store {#query_vector_store}

After creating the vector store and adding the necessary documents, you can perform search queries during chain or agent execution.

### Direct Query {#query_directly}

#### Similarity Search

A simple similarity search can be performed as follows:

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

Result:


```shell
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```


#### Similarity Search with Score

You can also perform a search with a score:

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

Result:


```shell
* [SIM=0.595] The weather forecast for tomorrow is cloudy and overcast, with a high of 62 degrees. [{'source': 'news'}]
* [SIM=0.212] I had chocolate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* [SIM=0.118] Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
```


### Filtering {#filtering}

Search using filters is performed as follows:

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

Result:


```shell
* I had chocolate chip pancakes and scrambled eggs for breakfast this morning. [{'source': 'tweet'}]
* Wow! That was an amazing movie. I can't wait to see it again. [{'source': 'tweet'}]
* Building an exciting new project with LangChain - come check it out! [{'source': 'tweet'}]
* LangGraph is the best framework for building stateful, agentic applications! [{'source': 'tweet'}]
```


### Query via Retriever Transformation {#query_by_turning_into_retriever}

The vector store can be transformed into a retriever for simplified use in chains.

An example is shown below:

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

Result:


```shell
* Robbers broke into the city bank and stole $1 million in cash. [{'source': 'news'}]
* The stock market is down 500 points today due to fears of a recession. [{'source': 'news'}]
```

# Mem0

[Mem0](https://mem0.ai/) is a long-term memory layer for AI agents and LLM applications. Mem0 extracts meaningful facts about the user from conversations, stores them as embeddings, and on subsequent queries finds relevant memories by semantic similarity. This allows the agent to "remember" the user's preferences and context across sessions.

Integration with {{ ydb-short-name }} allows using {{ ydb-short-name }} as a vector store for Mem0 memory — without a separate specialized vector database. The integration works on top of [LangChain](langchain.md): Mem0 supports LangChain vector stores as a backend, and {{ ydb-short-name }} provides a compatible store via the `langchain-ydb` package.

{% note info %}

In addition to the vector store, Mem0 uses a language model (LLM) to extract facts from the conversation and an embedding model to vectorize them. The examples below use OpenAI models (requires the `OPENAI_API_KEY` environment variable), but any [Mem0-supported provider](https://docs.mem0.ai/components/llms/overview) will work.

{% endnote %}

## Installation {#setup}

To use this integration, install a local {{ ydb-short-name }}. For more information, see [{#T}](../../quickstart.md#install).

Install Mem0 and the {{ ydb-short-name }} integration package with LangChain:


```shell
pip install -qU mem0ai langchain-ydb langchain-openai
```


## Initialization {#initialization}

Create a {{ ydb-short-name }} vector store using `langchain-ydb` and pass it to the Mem0 configuration with the `langchain` provider:

The key to OpenAI models is expected in the `OPENAI_API_KEY` environment variable — set it in advance in your environment or secret manager.


```python
from langchain_openai import OpenAIEmbeddings
from langchain_ydb.vectorstores import YDB, YDBSearchStrategy, YDBSettings
from mem0 import Memory

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

vector_store = YDB(
    embeddings,
    config=YDBSettings(
        host="localhost",
        port=2136,
        database="/local",
        table="mem0_memories",
        strategy=YDBSearchStrategy.COSINE_SIMILARITY,
    ),
)

memory = Memory.from_config({
    "vector_store": {
        "provider": "langchain",
        "config": {"client": vector_store},
    },
    "embedder": {
        "provider": "langchain",
        "config": {"model": embeddings},
    },
})
```


## Saving facts {#add}

Pass the conversation messages to Mem0 — the model will extract facts from them and save them to the {{ ydb-short-name }} table. Memory is shared between users using `user_id`:


```python
memory.add(
    [
        {"role": "user", "content": "Привет! Меня зовут Алиса, я работаю архитектором."},
        {"role": "assistant", "content": "Рада знакомству, Алиса!"},
        {"role": "user", "content": "Предпочитаю примеры на Python."},
    ],
    user_id="alice",
)
```


## Memory search {#search}

Before forming a response, the agent searches for relevant memories — Mem0 performs a vector search in {{ ydb-short-name }} and returns facts ordered by similarity:


```python
results = memory.search("На каком языке показывать примеры?", user_id="alice")
for item in results["results"]:
    print(f"{item['score']:.3f}  {item['memory']}")
```


Result:


```text
0.512  Prefers examples in Python
0.231  Works as an architect
```


## Viewing and deletion {#manage}

User memories can be retrieved or deleted entirely:


```python
# all user memories
for item in memory.get_all(user_id="alice")["results"]:
    print(item["memory"])

# delete all user memories
memory.delete_all(user_id="alice")
```

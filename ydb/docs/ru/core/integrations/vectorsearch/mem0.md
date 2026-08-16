# Mem0

[Mem0](https://mem0.ai/) — это слой долговременной памяти для AI-агентов и LLM-приложений. Mem0 извлекает из диалогов значимые факты о пользователе, сохраняет их в виде эмбеддингов и при следующих обращениях находит релевантные воспоминания по семантическому сходству. Благодаря этому агент «помнит» предпочтения и контекст пользователя между сессиями.

Интеграция с {{ ydb-short-name }} позволяет использовать {{ ydb-short-name }} в качестве векторного хранилища для памяти Mem0 — без отдельной специализированной векторной базы данных. Интеграция работает поверх [LangChain](langchain.md): Mem0 поддерживает векторные хранилища LangChain в качестве бэкенда, а {{ ydb-short-name }} предоставляет совместимое хранилище через пакет `langchain-ydb`.

{% note info %}

Помимо векторного хранилища, Mem0 использует языковую модель (LLM) для извлечения фактов из диалога и эмбеддинг-модель для их векторизации. В примерах ниже используются модели OpenAI (требуется переменная окружения `OPENAI_API_KEY`), но подойдёт любой [поддерживаемый Mem0 провайдер](https://docs.mem0.ai/components/llms/overview).

{% endnote %}

## Установка {#setup}

Для использования этой интеграции установите локальный {{ ydb-short-name }}. Для получения дополнительной информации см. [{#T}](../../quickstart.md#install).

Установите Mem0 и пакет интеграции {{ ydb-short-name }} с LangChain:

```shell
pip install -qU mem0ai langchain-ydb langchain-openai
```

## Инициализация {#initialization}

Создайте векторное хранилище {{ ydb-short-name }} средствами `langchain-ydb` и передайте его в конфигурацию Mem0 с провайдером `langchain`:

Ключ к моделям OpenAI ожидается в переменной окружения `OPENAI_API_KEY` — задайте её заранее в окружении или менеджере секретов.

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

## Сохранение фактов {#add}

Передайте Mem0 сообщения диалога — модель сама выделит из них факты и сохранит в таблицу {{ ydb-short-name }}. Память разделяется между пользователями с помощью `user_id`:

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

## Поиск по памяти {#search}

Перед формированием ответа агент ищет релевантные воспоминания — Mem0 выполняет векторный поиск в {{ ydb-short-name }} и возвращает факты, упорядоченные по сходству:

```python
results = memory.search("На каком языке показывать примеры?", user_id="alice")
for item in results["results"]:
    print(f"{item['score']:.3f}  {item['memory']}")
```

Результат:

```text
0.512  Предпочитает примеры на Python
0.231  Работает архитектором
```

## Просмотр и удаление {#manage}

Воспоминания пользователя можно получить или удалить целиком:

```python
# все воспоминания пользователя
for item in memory.get_all(user_id="alice")["results"]:
    print(item["memory"])

# удалить все воспоминания пользователя
memory.delete_all(user_id="alice")
```

# RAG Индекс для Триажа Тестов

Этот модуль реализует RAG (Retrieval-Augmented Generation) индекс для поиска релевантного кода при триаже тестов.

**📖 Для пошаговой инструкции см. [HOWTO.md](HOWTO.md)**

## Структура

```
triage/
├── build_rag_index.py      # Скрипт для построения индекса
├── rag_client.py           # Библиотека для работы с RAG индексом (класс RAGSearch)
├── rag_search.py           # CLI-утилита для быстрого поиска
├── index_config.json       # Конфигурация индексации
├── requirements_rag.txt    # Зависимости
└── rag_index/              # Директория с индексом
    ├── faiss_index/        # FAISS векторное хранилище
    ├── metadata.jsonl      # Метаданные всех чанков
    └── stats.json          # Статистика индексации
```

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements_rag.txt
```

### 2. Построение индекса

```bash
# Индексация всего репозитория
python3 build_rag_index.py --repo /path/to/repo --output ./rag_index

# Индексация с конфигурацией
python3 build_rag_index.py --repo /path/to/repo --output ./rag_index --config index_config.json
```

### 3. Поиск в индексе

```bash
# Простой поиск
python3 rag_search.py --query "test_etl_pipeline"

# Поиск с фильтрами
python3 rag_search.py --query "build graphs" --path-contains ".github/actions" --language yaml

# Показать полное содержимое
python3 rag_search.py --query "your query" --full-content
```

## Использование в Python коде

### Базовый поиск

```python
from rag_client import RAGSearch

# Загрузка индекса
search = RAGSearch("./rag_index/faiss_index")

# Простой поиск
results = search.search("test_etl_pipeline", k=5)

for result in results:
    print(f"{result.path}: {result.symbol}")
    print(f"Релевантность: {result.relevance} (score: {result.score:.4f})")
    print(result.content[:200])
```

### Гибридный поиск с фильтрами

```python
from rag_client import RAGSearch

search = RAGSearch("./rag_index/faiss_index")

# Гибридный поиск (векторный + точное совпадение)
results = search.hybrid_search(
    query="If true, compares build graphs",
    k=5,
    path_filter=".github/actions",
    language_filter="yaml"
)

for result in results:
    print(f"{result.path} ({result.relevance})")
```

### Получение контекста для теста (для triage-скрипта)

```python
from rag_client import RAGSearch

search = RAGSearch("./rag_index/faiss_index")

# Получить контекст для теста
test_name = "test_etl_pipeline"
test_log = """
AssertionError: Expected 100 records, got 95
  File "test_etl.py", line 42, in test_pipeline
    assert len(results) == 100
"""

context = search.get_context_for_test(
    test_name=test_name,
    log=test_log,
    k=5,
    prefer_test_files=True  # Приоритет тестовым файлам
)

print(context)
# Вывод:
# === Контекст 1 (релевантность: высокая, score: 0.3421) ===
# Файл: ydb/library/yql/dq/opt/ut/dq_opt_hypergraph_ut.cpp
# Символ: test_etl_pipeline()
# Тип: test
# Язык: python
# ---
# [содержимое чанка]
```

## API Reference

### `RAGSearch`

#### `__init__(index_path: str)`

Загружает FAISS индекс из указанной директории.

**Параметры:**
- `index_path`: Путь к директории с FAISS индексом (содержит `index.faiss` и `index.pkl`)

#### `get_context_for_test(test_name: str, log: str, k: int = 5, prefer_test_files: bool = True, max_log_length: int = 500) -> str`

Получает релевантный контекст для теста. Используется в triage-скрипте.

**Параметры:**
- `test_name`: Имя теста
- `log`: Лог ошибки теста
- `k`: Количество чанков для возврата
- `prefer_test_files`: Приоритет файлам с `kind="test"`
- `max_log_length`: Максимальная длина лога для запроса

**Возвращает:** Строка с форматированным контекстом для промпта LLM

#### `hybrid_search(query: str, k: int = 5, exact_match_boost: float = 2.0, path_filter: Optional[str] = None, language_filter: Optional[str] = None) -> List[SearchResult]`

Гибридный поиск: комбинация векторного поиска и точного совпадения подстроки.

**Параметры:**
- `query`: Поисковый запрос
- `k`: Количество результатов
- `exact_match_boost`: Множитель для улучшения score точных совпадений
- `path_filter`: Фильтр по подстроке в пути (например, ".github/actions")
- `language_filter`: Фильтр по языку (например, "yaml", "python")

**Возвращает:** Список `SearchResult` с отсортированными результатами

#### `search(query: str, k: int = 5, use_hybrid: bool = True, path_filter: Optional[str] = None, language_filter: Optional[str] = None) -> List[SearchResult]`

Универсальный метод поиска.

**Параметры:**
- `query`: Поисковый запрос
- `k`: Количество результатов
- `use_hybrid`: Использовать гибридный поиск (True) или только векторный (False)
- `path_filter`: Фильтр по подстроке в пути
- `language_filter`: Фильтр по языку

**Возвращает:** Список `SearchResult`

### `SearchResult`

Dataclass с результатами поиска:

- `path: str` - Путь к файлу
- `symbol: Optional[str]` - Имя функции/класса/символа
- `kind: str` - Тип: "test", "prod", "build", "config"
- `language: str` - Язык: "python", "cpp", "yaml", и т.д.
- `target: Optional[str]` - Таргет (для ya.make)
- `content: str` - Содержимое чанка
- `score: float` - Score релевантности (меньший = выше релевантность)
- `relevance: str` - "высокая", "средняя", "низкая"

## Примеры использования в triage-скрипте

```python
from rag_client import RAGSearch

# Инициализация
search = RAGSearch("./rag_index/faiss_index")

# Для каждого упавшего теста
for test in failed_tests:
    # Получаем контекст
    context = search.get_context_for_test(
        test_name=test.name,
        log=test.error_log,
        k=5,
        prefer_test_files=True
    )
    
    # Формируем промпт для LLM
    prompt = f"""
Проанализируй упавший тест и определи категорию: BUG / FLAKY / INFRA

Тест: {test.name}
Лог ошибки:
{test.error_log}

Релевантный код:
{context}

Ответь в формате:
CATEGORY: <BUG|FLAKY|INFRA>
REASON: <краткое объяснение>
"""
    
    # Отправляем в LLM
    response = llm_client.generate(prompt)
    # ...
```

## Возобновляемая индексация

Если индексация прервалась, можно продолжить с места остановки:

```bash
# Продолжить после сохранения документов
python3 build_rag_index.py --repo /path/to/repo --output ./rag_index --skip-parsing

# Продолжить после генерации эмбеддингов
python3 build_rag_index.py --repo /path/to/repo --output ./rag_index --skip-parsing --skip-embeddings
```

## Конфигурация

Файл `index_config.json` позволяет настроить:
- `include_patterns` - паттерны файлов для индексации
- `exclude_patterns` - паттерны для исключения
- `exclude_directories` - директории для исключения
- `exclude_file_extensions` - расширения файлов для исключения

## Статистика

После индексации создается `rag_index/stats.json`:

```json
{
  "python": 88020,
  "cpp": 113681,
  "yamake": 3474,
  "protobuf": 8117,
  "yaml": 482,
  "config": 2738,
  "markdown": 15708,
  "total_documents": 232220,
  "total_files": 39794
}
```

## Следующие шаги (Этап 4)

После завершения Этапа 3 (RAG индекс), следующий этап:

1. **Подключение Yandex LLM API** - заменить временный LLM на YandexGPT/DeepSeek
2. **Интеграция с triage-скриптом** - использовать `get_context_for_test()` в промптах
3. **Форматирование ответов** - структурированный вывод (JSON) для категоризации тестов

## Troubleshooting

### Индекс не загружается

```bash
# Проверьте наличие файлов
ls -la rag_index/faiss_index/
# Должны быть: index.faiss и index.pkl
```

### Низкая релевантность результатов

- Увеличьте `k` для получения большего количества кандидатов
- Используйте `hybrid_search` вместо простого векторного поиска
- Добавьте фильтры (`path_filter`, `language_filter`) для сужения области поиска

### Ошибка импорта LangChain

```bash
# Установите правильные версии
pip install langchain>=1.0.0 langchain-community>=0.0.20 langchain-core>=0.1.0
```

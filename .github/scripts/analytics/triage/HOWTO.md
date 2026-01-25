# Как использовать RAG индекс — Пошаговая инструкция

## 📋 Порядок использования

### Шаг 1: Первая индексация (один раз)

**Цель:** Построить индекс всего репозитория

```bash
cd /Users/kirrysin/projects/ydb/.github/scripts/analytics/triage

# Индексация всего репозитория
python3 build_rag_index.py \
  --repo /Users/kirrysin/projects/ydb \
  --output ./rag_index
```

**Что происходит:**
- Сканирует все файлы (Python, C++, ya.make, YAML, и т.д.)
- Парсит их на семантические чанки
- Генерирует эмбеддинги
- Сохраняет FAISS индекс и метаданные

**Время:** ~2-3 часа для большого репозитория

**Результат:**
```
rag_index/
├── faiss_index/          # Векторное хранилище
├── metadata.jsonl        # Метаданные
├── stats.json           # Статистика
├── documents.pkl        # Промежуточные данные
└── embeddings.npy       # Промежуточные данные
```

---

### Шаг 2: Поиск в индексе (постоянно)

**Цель:** Найти релевантный код по запросу

#### Вариант A: Быстрый поиск через CLI

```bash
# Простой поиск
python3 rag_search.py --query "test_etl_pipeline"

# Поиск с фильтрами
python3 rag_search.py \
  --query "compares build graphs" \
  --path-contains ".github/actions" \
  --language yaml \
  --k 10

# Показать полное содержимое
python3 rag_search.py \
  --query "your query" \
  --full-content
```

#### Вариант B: Использование в Python-скрипте

```python
from rag_client import RAGSearch

# Загружаем индекс
search = RAGSearch("./rag_index/faiss_index")

# Простой поиск
results = search.search("test_etl_pipeline", k=5)
for result in results:
    print(f"{result.path}: {result.score:.4f}")

# Получить контекст для теста (для triage-скрипта)
context = search.get_context_for_test(
    test_name="test_etl_pipeline",
    log="AssertionError: Expected 100 records",
    k=5,
    prefer_test_files=True
)
print(context)
```

---

### Шаг 3: Обновление индекса (при изменении кода)

**Цель:** Переиндексировать после изменений в репозитории

#### Полная переиндексация

```bash
# Удалить старый индекс (опционально)
rm -rf rag_index/faiss_index

# Переиндексировать
python3 build_rag_index.py \
  --repo /Users/kirrysin/projects/ydb \
  --output ./rag_index
```

#### Возобновление после сбоя

```bash
# Если индексация прервалась после парсинга
python3 build_rag_index.py \
  --repo /Users/kirrysin/projects/ydb \
  --output ./rag_index \
  --skip-parsing

# Если индексация прервалась после генерации эмбеддингов
python3 build_rag_index.py \
  --repo /Users/kirrysin/projects/ydb \
  --output ./rag_index \
  --skip-parsing \
  --skip-embeddings
```

---

## 🔄 Типичные сценарии использования

### Сценарий 1: Первый запуск

```bash
# 1. Установить зависимости
pip install -r requirements_rag.txt

# 2. Построить индекс
python3 build_rag_index.py \
  --repo /Users/kirrysin/projects/ydb \
  --output ./rag_index

# 3. Проверить, что индекс создан
ls -la rag_index/faiss_index/

# 4. Протестировать поиск
python3 rag_search.py --query "test" --k 5
```

### Сценарий 2: Поиск кода для триажа теста

```bash
# Найти релевантный код для упавшего теста
python3 rag_search.py \
  --query "test_etl_pipeline AssertionError" \
  --k 10 \
  --prefer-test-files
```

Или в Python:

```python
from rag_client import RAGSearch

search = RAGSearch("./rag_index/faiss_index")
context = search.get_context_for_test(
    test_name="test_etl_pipeline",
    log="AssertionError: Expected 100 records, got 95",
    k=5
)

# Использовать context в промпте для LLM
prompt = f"""
Проанализируй тест:
{test_name}
Лог: {log}

Релевантный код:
{context}
"""
```

### Сценарий 3: Поиск конкретного файла

```bash
# Найти файл по пути
python3 rag_search.py \
  --query "increment" \
  --path-contains ".github/actions" \
  --language yaml
```

### Сценарий 4: Интеграция в triage-скрипт (Этап 4)

```python
from rag_client import RAGSearch

class TriageAgent:
    def __init__(self):
        self.rag = RAGSearch("./rag_index/faiss_index")
    
    def triage_test(self, test_name, test_log):
        # Получаем контекст
        context = self.rag.get_context_for_test(
            test_name=test_name,
            log=test_log,
            k=5,
            prefer_test_files=True
        )
        
        # Формируем промпт для LLM
        prompt = f"""
        Проанализируй упавший тест и определи категорию: BUG / FLAKY / INFRA
        
        Тест: {test_name}
        Лог: {test_log}
        
        Релевантный код:
        {context}
        """
        
        # Отправляем в LLM (YandexGPT/DeepSeek)
        response = self.llm_client.generate(prompt)
        return response
```

---

## 📊 Проверка состояния индекса

```bash
# Посмотреть статистику
cat rag_index/stats.json

# Проверить количество документов
wc -l rag_index/metadata.jsonl

# Проверить размер индекса
du -sh rag_index/
```

---

## 🛠️ Устранение проблем

### Индекс не найден

```bash
# Проверить наличие файлов
ls -la rag_index/faiss_index/
# Должны быть: index.faiss и index.pkl

# Если нет - переиндексировать
python3 build_rag_index.py --repo /path/to/repo --output ./rag_index
```

### Поиск не находит файлы

```bash
# Увеличить количество кандидатов
python3 rag_search.py --query "your query" --k 20

# Убрать фильтры для проверки
python3 rag_search.py --query "your query"

# Использовать более общий запрос
python3 rag_search.py --query "increment"  # вместо "compares build graphs"
```

### Ошибки импорта

```bash
# Установить зависимости
pip install -r requirements_rag.txt

# Проверить версии
pip list | grep langchain
```

---

## 📝 Чеклист для Этапа 3

- [ ] Установлены зависимости (`requirements_rag.txt`)
- [ ] Построен индекс (`build_rag_index.py`)
- [ ] Протестирован поиск (`rag_search.py`)
- [ ] Проверена статистика (`stats.json`)
- [ ] Готово к использованию в triage-скрипте (`rag_client.py`)

---

## 🚀 Следующий шаг: Этап 4

После завершения Этапа 3, переходим к Этапу 4:

1. **Подключить Yandex LLM API**
2. **Создать triage-скрипт**, который использует `rag_client.py`
3. **Интегрировать** поиск контекста в промпты для LLM

Пример использования в triage-скрипте:

```python
from rag_client import RAGSearch

# В triage-скрипте
rag = RAGSearch("./rag_index/faiss_index")

for test in failed_tests:
    context = rag.get_context_for_test(test.name, test.log, k=5)
    # ... отправка в LLM
```

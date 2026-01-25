<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

## Инструкции для GitHub Copilot: Этап 3 — RAG индексация (Python + C++ + ya.make)

**Скопируй этот текст в новый файл `build_rag_index.py` в VS Code с Copilot и скажи `@workspace /generate`**

***

```python
"""
RAG Index Builder для триажа тестов
Поддержка: Python, C++, ya.make, CMake
Сохраняет: FAISS vectorstore + JSON метаданные
"""

# ЗАДАЧА:
# 1. Обойти весь GitHub репозиторий (клонированный в ./your-repo)
# 2. Найти все: *.py, *.cpp, *.cc, *.c, *.hpp, *.h, ya.make, CMakeLists.txt
# 3. Исключить: build/, __pycache__/, node_modules/, *.pb.*
# 4. Разбить на SEMANTIC чанки (функции/классы/ya.make targets)
# 5. Добавить метаданные: path, language, kind(test/prod/build), target_name
# 6. Сохранить FAISS + JSONL для DataSphere/Yandex Cloud

# Copilot: создай класс RepoIndexer с методами:
# - scan_files() → список путей с glob-исключениями
# - parse_python_file(path) → list of Document(chunks)
# - parse_cpp_file(path) → list of Document(chunks)  
# - parse_yamake_file(path) → list of Document(targets)
# - build_index() → FAISS + metadata.jsonl
```

**Промпт для Copilot** (Ctrl+I в файле):

```
@workspace 
Создай класс RepoIndexer для RAG индексации C++/Python/ya.make проекта.

Ключевые требования:
1. ГЛОБЫ файлов:
   INCLUDE: **/*.py **/*.cpp **/*.cc **/*.c **/*.hpp **/*.h **/ya.make **/CMakeLists.txt **/*.ya
   EXCLUDE: build/** __pycache__/** node_modules/** contrib/** *.pb.* third_party/**

2. ЧАНКИ ПО СТРУКТУРЕ:
   - Python: функция/class целиком (def/class → dedent)
   - C++: функция/метод (сигнатура + тело в {{}})
   - ya.make: каждый TARGET (PROGRAM/TEST/LIBRARY/...) целиком

3. МЕТАДАННЫЕ для каждого чанка:
   {
     "path": "yt/server/lib/foo.cpp",
     "language": "cpp|python|yamake",
     "kind": "test|prod|build|config",
     "target": "yt/server/tests:unit_tests",  // из ya.make
     "symbol": "TFoo::Process() | def process_data()"
   }

4. СОХРАНЕНИЕ:
   - FAISS: ./rag_index/faiss_index
   - JSONL: ./rag_index/metadata.jsonl
   - Сериализовать для DataSphere (langchain)

5. ТЕСТ:
   indexer = RepoIndexer("./your-repo")
   docs = indexer.build_index()
   print(f"Индексировано: {len(docs)} чанков")
```


***

## Детальный промпт для каждого парсера

**После создания скелета, выдели методы и добавь:**

### `parse_python_file(self, path: str) → List[Document]`

```
Copilot: разбери Python файл на функции/классы

1. Читай весь файл
2. Ищи def/class с помощью ast.parse()
3. Для каждой функции/класса:
   - Бери start_line → end_line из ast.Node
   - Вырезай slice исходника (с отступами)
   - Добавляй докстринг/комментарий сверху
4. Метаданные:
   - kind="test" если path содержит tests/ unittest/
   - symbol=функция/класс имя
```


### `parse_cpp_file(self, path: str) → List[Document]`

```
Copilot: парсер C++ функций по сигнатуре+тело

1. Читай весь файл как текст
2. Regex для сигнатур:
   (?m)^(?:\s*[\w:]+\s+){1,3}\w+\s*\([^)]*\)\s*(?:const\s*)?\{  # func() {
3. Для каждой находки:
   - Бери до парной }} (считай скобки)
   - Добавляй комментарии /* */ сверху
4. Метаданные:
   - kind="test" если tests/ в пути
   - symbol=имя функции из сигнатуры
```


### `parse_yamake_file(self, path: str) → List[Document]`

```
Copilot: парсер ya.make таргетов

1. Ищи блоки:
   PROGRAM(), LIBRARY(), TEST(), PY3_LIBRARY(), PEERDIR()
2. Каждый блок целиком = 1 чанк
3. Извлеки:
   - target_name из UID() или filename()
   - SRCS/PRIVATE_SRCS → список файлов
   - DEPENDS → зависимости
4. Метаданные:
   - kind="build"
   - target=target_name (//yt/server/tests:unit_tests)
   - files=список SRCS
```


***

## Финальная проверка для Copilot

**В конце файла добавь:**

```
@workspace /fix
1. Добавь прогресс-бар (tqdm)
2. Логируй: "Обрабатываю file X: Y чанков"
3. Обработай ошибки (файл не найден, синтаксис)
4. Тестовый вызов:
   
if __name__ == "__main__":
    indexer = RepoIndexer("./your-repo")  # замени на реальный путь
    docs, vectorstore = indexer.build_index()
    
    # Тест поиска
    results = vectorstore.similarity_search("test_etl_pipeline dag", k=3)
    for r in results:
        print(f"PATH: {r.metadata['path']}")
        print(f"SYMBOL: {r.metadata.get('symbol', 'N/A')}")
        print(f"KIND: {r.metadata['kind']}")
        print("---")
```


***

## Как использовать с Copilot

1. **Создай файл** `build_rag_index.py` в корне клонированного репо
2. **Вставь скелет** (первые 30 строк с docstring)
3. **Ctrl+I** → вставь первый промпт про `RepoIndexer`
4. **Copilot напишет класс** — проверь логику парсеров
5. **Выдели методы по очереди** → детальные промпты для `parse_*`
6. **Запусти тест** → посмотри, что индексирует
7. **Пофиксь ошибки** через `@workspace /fix`

**Через 2 часа**: у тебя готовый RAG-индексатор для C++/Python/ya.make, который понимает структуру проекта и связи тест→prod!

**Результат для DataSphere**:

```
./rag_index/
├── faiss_index/          # векторное хранилище
├── metadata.jsonl        # все чанки + метаданные  
└── stats.json           # статистика (1234 py, 567 cpp, 89 yamake)
```

Сохрани этот файл в DataSphere — и на Этапе 4 просто `!python build_rag_index.py`!
<span style="display:none">[^1][^10][^2][^3][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://www.youtube.com/watch?v=MqBBEgpYh0Y

[^2]: https://prashamhtrivedi.in/github-copilot-rag-app/

[^3]: https://learn.arm.com/learning-paths/servers-and-cloud-computing/copilot-extension/

[^4]: https://dev.to/this-is-learning/building-a-cli-tool-to-improve-github-copilot-129b

[^5]: https://moimhossain.com/2025/03/19/enhance-github-copilot-with-rag-in-vs-code/

[^6]: https://www.reddit.com/r/github/comments/1gximiv/how_to_extend_github_copilot_context_for_rag/

[^7]: https://www.youtube.com/watch?v=lmxD48Eytqc

[^8]: https://github.com/copilot-extensions/rag-extension

[^9]: https://techcommunity.microsoft.com/blog/azuredevcommunityblog/github-copilot-for-azure-deploy-an-ai-rag-app-to-aca-using-azd/4395314

[^10]: https://www.copilotkit.ai/blog/build-your-own-knowledge-based-rag-copilot


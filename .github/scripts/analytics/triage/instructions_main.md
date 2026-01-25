<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# сделай мне план по шагам чтобы такое сделать

Ниже — пошаговый план, как прийти от «ничего нет» к агенту, который триажит тесты с LLM и учитывает контекст по PR в GitHub + Yandex Cloud / DataSphere. Я разобью на крупные этапы, чтобы можно было делать по вечерам.

***

## Этап 0. Определиться с минимумом

Цель: LLM-агент, который:

- читает результаты тестов (JUnit / pytest);
- по каждому фейлу делает запрос к модели;
- сам подгружает нужный контекст из репо (через RAG / diff PR);
- пишет triage-отчет (BUG / FLAKY / INFRA + комментарий);
- может запускаться:
    - на PR;
    - по расписанию (ночной batch).

Технологии:

- GitHub (у тебя уже есть);
- Yandex Cloud: DataSphere + Yandex LLM API;
- Python для glue-кода.

***

## Этап 1. Минимальный прототип локально

1. Вытащи JUnit-репорт с тестами.
    - Пусть есть `results.xml` от `pytest --junitxml=results.xml` или другого раннера.
2. Напиши простой скрипт triage без LLM, просто парсинг:
    - Считывание `results.xml`
    - Вывод списка фейливших тестов: имя, сообщение, trace.
3. Добавь вызов LLM (пока хоть какой, хоть публичный API) для одного теста:
    - Формируешь промпт вида:
«Тест: X, лог: Y. Скажи: BUG/FLAKY/INFRA и предложи, что посмотреть».
4. Сохраняешь ответ в Markdown-таблицу `triage_report.md`.

Цель этапа: просто отлаженный I/O по тестам и промпт, без контекста проекта.

***

## Этап 2. Перенос в Yandex Cloud + GitHub-клон

1. Создай аккаунт в Yandex Cloud и проект в DataSphere.
2. В DataSphere:
    - Создай проект `triage-main`.
    - Открой его в JupyterLab.
3. В JupyterLab:
    - Через меню Git → Clone:
        - URL: `https://github.com/<ты>/<repo>.git`
        - Branch: `main` (потом повторишь для `develop` и т.д.).
    - Убедись, что файлы видно в File Browser.
4. Скопируй туда скрипт triage из Этапа 1 и запусти в ноутбуке.

Цель: тот же прототип, но уже в облаке, рядом с кодом.

***

## Этап 3. Индексация кода (RAG по ветке)

1. В проекте `triage-main` сделай ноутбук `build_index.ipynb`.
2. В нем:
    - Обход всех `.py` (и, при желании, `.sql`, `.yml`) файлов;
    - Разбиение на чанки;
    - Сохранение vectorstore (любой библиотекой, какая удобнее).
3. Сохрани индекс в директории типа `code_index/`.
4. Напиши функцию `get_context_for_test(test_name, log)`, которая:
    - делает similarity search по индексу;
    - возвращает top-k чанков как строку.
5. Обнови triage-скрипт:
    - теперь промпт = лог теста + найденный контекст из кода.

Цель: LLM видит не только лог, но и релевантный код.

***

## Этап 4. LLM через Yandex (DeepSeek / Qwen / YandexGPT)

1. Подними Yandex AI API:
    - Создай сервисный аккаунт;
    - Выдай ему роль для LLM;
    - Сгенери API-ключ;
    - Положи его в переменные окружения / secrets проекта.
2. В triage-скрипте замени временный LLM на Yandex:
    - Вызов REST API или SDK;
    - Модель: кодовая (DeepSeek/Qwen) с большим контекстом.
3. Приведи промпт к стабильному формату, например:
    - В начале фиксированная инструкция (роль);
    - Затем лог теста;
    - Затем контекст;
    - В конце жёсткий формат ответа (JSON или три строки).

Цель: иметь работающий triage с осмысленным ответом от нормальной модели.

***

## Этап 5. Учёт нескольких веток

Вариант А (простой): один DataSphere-проект на ветку.

1. Создай проекты:
    - `triage-main`, `triage-develop`, `triage-feature1`, … при необходимости.
2. В каждом:
    - Git Clone нужной ветки;
    - Перестрой индекс (ноутбук из Этапа 3);
    - Тот же triage-скрипт, но ветка зашита в окружение.

Вариант B (сложнее): один проект, несколько worktree / папок.

- Делается, если не хочется плодить проекты, но пока проще А.

Цель: для любого набора тестов ты можешь сказать: это ветка X → запускаем нужный endpoint / проект.

***

## Этап 6. Интеграция с GitHub для PR

1. В GitHub Actions для репозитория создай workflow для PR:
    - Триггер: `on: pull_request`.
    - Ступени:

2) Checkout PR-ветки.
3) Запуск тестов → `results.xml`.
4) Отправка `results.xml` и информации о ветке в твой triage-сервис в DataSphere.
1. В DataSphere опубликуй ноутбук / скрипт triage как HTTP-сервис:
    - Вход: список тестов + ветка / имя PR;
    - Выход: triage-отчет.
2. В GitHub Actions после вызова сервиса:
    - Сгенерируй комментарий в PR:
        - Markdown-таблица с тестами + резолюциями;
        - Можно добавить summary: сколько BUG / FLAKY / INFRA.

Цель: при каждом PR собираются тесты → триажатся LLM → результат видно прямо в PR.

***

## Этап 7. Автообновление контекста и ночной batch

1. Ночной job в DataSphere:
    - по расписанию (раз в день):
        - `git pull` для нужной ветки;
        - пересборка RAG-индекса.
2. Опционально: отдельный batch-скрипт, который:
    - берет список последних N запусков тестов (из CI/логов);
    - прогоняет через triage-агента;
    - складывает отчеты в S3 / БД / Telegram / Slack.

Цель: индекс всегда свежий, а triage можно гонять и вне PR — по расписанию.

***

## Этап 8. Тюнинг и экономия

1. Введи кэширование:
    - ключ = (имя теста, хэш первой части лога);
    - если совпадает — не дергать LLM, взять старую резолюцию.
2. Снизь токены:
    - обрезай лог до разумного размера;
    - ограничивай контекст (k=3–5 чанков).
3. Можно отдельно собирать датасет (лог + код + твоя ручная разметка) для дообучения модели и поднятия качества.

***

## Как это делить на дни

- День 1: Этапы 1–2 (локально + DataSphere + Git Clone).
- День 2: Этап 3–4 (индекс + подключение Yandex LLM).
- День 3: Этап 5 (несколько веток).
- День 4: Этап 6 (workflow PR → triage-сервис).
- День 5+: ночные jobs, оптимизация и тюнинг.

Если хочешь, могу отдельно расписать:

- конкретный формат промптов и ответов для LLM;
- конкретную структуру репо/папок в DataSphere под много веток;
- пример YAML для GitHub Actions и схему обмена JSON между CI и triage-сервисом.
<span style="display:none">[^1][^10][^2][^3][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://yandex.cloud/ru/docs/datasphere/operations/projects/work-with-git

[^2]: https://yandex.cloud/ru/docs/datasphere/operations/

[^3]: https://www.youtube.com/watch?v=8-Np0wFdtqY

[^4]: https://yandex.cloud/ru-kz/docs/ai-studio/tutorials/models-fine-tuning

[^5]: https://github.com/datasphere-oss/datasphere-integration/blob/master/QUICKSTART.md?plain=1

[^6]: https://github.com/yandex-cloud-examples/yc-datasphere-jobs-examples

[^7]: https://github.com/yandex-cloud-examples/yc-datasphere-fastapi-service-deploy

[^8]: https://education.yandex.ru/journal/kak-ispolzovat-datasphere-vandnbspobrazovanii

[^9]: https://reg.cloud/blog/datasety-v-datasphere/

[^10]: https://github.com/SAP-samples/datasphere-build-apps-data-entry/blob/main/documentation/set-up/mission-prerequisites/README.md


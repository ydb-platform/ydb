# Статистика выполнения запросов и её анализ

Некоторые аспекты о работе со статистикой выполнения запросов:

- [Обзор](review.md)
- [Как получить графическое представление плана](how-to-get.md)
    - [UI](how-to-get.md#ui)
    - [ydb cli](how-to-get.md#cli)
    - [ydb_int](how-to-get.md#ydb_int)
    - [kqprun](how-to-get.md#kqprun)
- [Структура плана запроса](structure.md)
    - [Стадии](structure.md#stages)
    - [Каналы связи](structure.md#connections)
    - [Объединение](structure.md#join)
    - [Множественные выходы](structure.md#multiout)
    - [Составные графы](structure.md#complex)
- [Визуализация метрик](metrics.md)
    - [Параллельность](metrics.md#parallelism)
    - [Агрегаты](metrics.md#aggregates)
    - [Масштаб метрик](metrics.md#scale)
    - [Перекос данных](metrics.md#dataskew)
    - [Перекос времени](metrics.md#timeskew)
    - [Потребление CPU](metrics.md#cpu)
    - [Потребление памяти](metrics.md#memory)

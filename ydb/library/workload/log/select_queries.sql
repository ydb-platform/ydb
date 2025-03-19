-- Q0: Базовый подсчет всех записей
SELECT COUNT(*) FROM `{table}`;
-- Q1: Подсчет записей с определенным уровнем (аналог AdvEngineID <> 0)
SELECT COUNT(*) FROM `{table}` WHERE level <> 0;
-- Q2: Агрегация нескольких метрик
SELECT SUM(level), COUNT(*), AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE)) FROM `{table}`;
-- Q3: Средние значения
SELECT AVG(CAST(request_id AS Int64)) FROM `{table}`;
-- Q6: Временной диапазон логов
SELECT MIN(timestamp), MAX(timestamp) FROM `{table}`;
-- Q19: Поиск по конкретному request_id
SELECT request_id
FROM `{table}`
WHERE request_id = '435090932899640449';
-- Q20: Поиск по подстроке в message
SELECT COUNT(*)
FROM `{table}`
WHERE message LIKE '%error%';
-- Q23: Выборка с сортировкой по времени
SELECT *
FROM `{table}`
WHERE message LIKE '%error%'
ORDER BY timestamp
LIMIT 10;
-- Q24: Простая выборка сообщений
SELECT message
FROM `{table}`
WHERE message <> ''
ORDER BY timestamp
LIMIT 10;
-- Q25: Сортировка по сообщению
SELECT message
FROM `{table}`
WHERE message <> ''
ORDER BY message
LIMIT 10;
-- Q26: Сортировка по времени и сообщению
SELECT message
FROM `{table}`
WHERE message <> ''
ORDER BY timestamp, message
LIMIT 10;

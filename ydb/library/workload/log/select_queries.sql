-- Q0: Базовый подсчет всех записей
SELECT COUNT(*) FROM `{table}`;
-- Q1: Подсчет записей с определенным уровнем (аналог AdvEngineID <> 0)
SELECT COUNT(*) FROM `{table}` WHERE level <> 0;
-- Q2: Агрегация нескольких метрик
SELECT SUM(level), COUNT(*), AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE)) FROM `{table}`;
-- Q3: Средние значения
SELECT AVG(CAST(request_id AS Int64)) FROM `{table}`;
-- Q4: Уникальные значения
SELECT COUNT(DISTINCT request_id) FROM `{table}`;
-- Q5: Уникальные сообщения об ошибках
SELECT COUNT(DISTINCT message) FROM `{table}`;
-- Q6: Временной диапазон логов
SELECT MIN(timestamp), MAX(timestamp) FROM `{table}`;
-- Q7: Группировка по уровню важности
SELECT level, COUNT(*) as count
FROM `{table}`
WHERE level <> 0
GROUP BY level
ORDER BY count DESC;
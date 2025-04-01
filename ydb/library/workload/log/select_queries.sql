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
-- Q8: Топ-10 сервисов по уникальным request_id
SELECT service_name, COUNT(DISTINCT request_id) AS u
FROM `{table}`
GROUP BY service_name
ORDER BY u DESC
LIMIT 10;
-- Q9: Комплексная агрегация по сервисам
SELECT
    service_name,
    SUM(level),
    COUNT(*) AS c,
    AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE)),
    COUNT(DISTINCT request_id)
FROM `{table}`
GROUP BY service_name
ORDER BY c DESC
LIMIT 10;
-- Q10: Группировка по компонентам с подсчетом уникальных request_id
SELECT component, COUNT(DISTINCT request_id) AS u
FROM `{table}`
WHERE component <> ''
GROUP BY component
ORDER BY u DESC
LIMIT 10;
-- Q11: Группировка по компоненту и сервису
SELECT component, service_name, COUNT(DISTINCT request_id) AS u
FROM `{table}`
WHERE component <> ''
GROUP BY component, service_name
ORDER BY u DESC
LIMIT 10;
-- Q12: Топ сообщений об ошибках
SELECT message, COUNT(*) AS c
FROM `{table}`
WHERE message <> ''
GROUP BY message
ORDER BY c DESC
LIMIT 10;
-- Q13: Уникальные request_id по сообщениям
SELECT message, COUNT(DISTINCT request_id) AS u
FROM `{table}`
WHERE message <> ''
GROUP BY message
ORDER BY u DESC
LIMIT 10;
-- Q14: Группировка по уровню и сообщению
SELECT level, message, COUNT(*) AS c
FROM `{table}`
WHERE message <> ''
GROUP BY level, message
ORDER BY c DESC
LIMIT 10;
-- Q15: Топ по request_id
SELECT request_id, COUNT(*) as count
FROM `{table}`
GROUP BY request_id
ORDER BY count DESC
LIMIT 10;
-- Q16: Группировка по request_id и сообщению
SELECT request_id, message, COUNT(*) as count
FROM `{table}`
GROUP BY request_id, message
ORDER BY count DESC
LIMIT 10;
-- Q17: Простая группировка без сортировки
SELECT request_id, message, COUNT(*)
FROM `{table}`
GROUP BY request_id, message
LIMIT 10;
-- Q18: Группировка с извлечением минут
SELECT
    request_id,
    m,
    message,
    COUNT(*) as count
FROM `{table}`
GROUP BY request_id, CAST(CAST(timestamp AS Uint64) / 60000000 * 60000000 AS DateTime) AS m, message
ORDER BY count DESC
LIMIT 10;
-- Q19: Поиск по конкретному request_id
SELECT request_id
FROM `{table}`
WHERE request_id = '435090932899640449';
-- Q20: Поиск по подстроке в message
SELECT COUNT(*)
FROM `{table}`
WHERE message LIKE '%error%';
-- Q21: Анализ ошибок с URL
SELECT
    message,
    MIN(JSON_VALUE(metadata, "$.url")),
    COUNT(*) AS c
FROM `{table}`
WHERE JSON_VALUE(metadata, "$.url") LIKE '%api%'
    AND message <> ''
GROUP BY message
ORDER BY c DESC
LIMIT 10;
-- Q22: Сложный поиск с несколькими условиями
SELECT
    message,
    MIN(JSON_VALUE(metadata, "$.url")),
    MIN(JSON_VALUE(metadata, "$.title")),
    COUNT(*) AS c,
    COUNT(DISTINCT request_id)
FROM `{table}`
WHERE JSON_VALUE(metadata, "$.title") LIKE '%Error%'
    AND JSON_VALUE(metadata, "$.url") NOT LIKE '%api%'
    AND message <> ''
GROUP BY message
ORDER BY c DESC
LIMIT 10;
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
-- Q27: Анализ длины сообщений
SELECT
    service_name,
    AVG(Unicode::GetLength(message)) AS l,
    COUNT(*) AS c
FROM `{table}`
WHERE message <> ''
GROUP BY service_name
HAVING COUNT(*) > 1000
ORDER BY l DESC
LIMIT 25;
-- Q28: Анализ источников ошибок
SELECT
    component,
    AVG(Unicode::GetLength(message)) AS l,
    COUNT(*) AS c,
    MIN(message)
FROM `{table}`
WHERE message <> ''
GROUP BY component
HAVING COUNT(*) > 1000
ORDER BY l DESC
LIMIT 25;
-- Q29: Множественные суммы
SELECT
    SUM(level),
    SUM(level + 1),
    SUM(level + 2),
    SUM(level + 3),
    SUM(level + 4),
    SUM(level + 5),
    SUM(level + 6),
    SUM(level + 7),
    SUM(level + 8),
    SUM(level + 9),
    SUM(level + 10),
    SUM(level + 11),
    SUM(level + 12),
    SUM(level + 13),
    SUM(level + 14),
    SUM(level + 15),
    SUM(level + 16),
    SUM(level + 17),
    SUM(level + 18),
    SUM(level + 19),
    SUM(level + 20),
    SUM(level + 21),
    SUM(level + 22),
    SUM(level + 23),
    SUM(level + 24),
    SUM(level + 25),
    SUM(level + 26),
    SUM(level + 27),
    SUM(level + 28),
    SUM(level + 29),
    SUM(level + 30),
    SUM(level + 31),
    SUM(level + 32),
    SUM(level + 33),
    SUM(level + 34),
    SUM(level + 35),
    SUM(level + 36),
    SUM(level + 37),
    SUM(level + 38),
    SUM(level + 39),
    SUM(level + 40),
    SUM(level + 41),
    SUM(level + 42),
    SUM(level + 43),
    SUM(level + 44),
    SUM(level + 45),
    SUM(level + 46),
    SUM(level + 47),
    SUM(level + 48),
    SUM(level + 49),
    SUM(level + 50),
    SUM(level + 51),
    SUM(level + 52),
    SUM(level + 53),
    SUM(level + 54),
    SUM(level + 55),
    SUM(level + 56),
    SUM(level + 57),
    SUM(level + 58),
    SUM(level + 59),
    SUM(level + 60),
    SUM(level + 61),
    SUM(level + 62),
    SUM(level + 63),
    SUM(level + 64),
    SUM(level + 65),
    SUM(level + 66),
    SUM(level + 67),
    SUM(level + 68),
    SUM(level + 69),
    SUM(level + 70),
    SUM(level + 71),
    SUM(level + 72),
    SUM(level + 73),
    SUM(level + 74),
    SUM(level + 75),
    SUM(level + 76),
    SUM(level + 77),
    SUM(level + 78),
    SUM(level + 79),
    SUM(level + 80),
    SUM(level + 81),
    SUM(level + 82),
    SUM(level + 83),
    SUM(level + 84),
    SUM(level + 85),
    SUM(level + 86),
    SUM(level + 87),
    SUM(level + 88),
    SUM(level + 89)
FROM `{table}`;
-- Q30: Группировка по нескольким полям
SELECT
    level,
    client_ip,
    COUNT(*) AS c,
    SUM(CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32)),
    AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE))
FROM `{table}`
WHERE message <> ''
GROUP BY level, JSON_VALUE(metadata, "$.client_ip") AS client_ip
ORDER BY c DESC
LIMIT 10;
-- Q31: Аналогичная группировка с другими полями
SELECT
    request_id,
    client_ip,
COUNT(*) AS c,
    SUM(CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32)),
    AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE))
FROM `{table}`
WHERE message <> ''
GROUP BY request_id, JSON_VALUE(metadata, "$.client_ip") AS client_ip
ORDER BY c DESC
LIMIT 10;
-- Q32: Похожая группировка без фильтра
SELECT
    request_id,
    client_ip,
    COUNT(*) AS c,
    SUM(CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32)),
    AVG(CAST(JSON_VALUE(metadata, "$.response_time") AS DOUBLE))
FROM `{table}`
GROUP BY request_id, JSON_VALUE(metadata, "$.client_ip") AS client_ip
ORDER BY c DESC
LIMIT 10;
-- Q33: Группировка по URL
SELECT
    url,
    COUNT(*) AS c
FROM `{table}`
GROUP BY JSON_VALUE(metadata, "$.url") AS url
ORDER BY c DESC
LIMIT 10;
-- Q34: Группировка с константой
SELECT
    1,
    url,
    COUNT(*) AS c
FROM `{table}`
GROUP BY JSON_VALUE(metadata, "$.url") AS url
ORDER BY c DESC
LIMIT 10;
-- Q35: Группировка по IP с вычислениями
SELECT
    client_ip
    client_ip_1,
    client_ip_2,
    client_ip_3,
    COUNT(*) AS c
FROM `{table}`
GROUP BY
    JSON_VALUE(metadata, "$.client_ip") AS client_ip,
    CAST(JSON_VALUE(metadata, "$.client_ip") AS Int64) - 1 AS client_ip_1,
    CAST(JSON_VALUE(metadata, "$.client_ip") AS Int64) - 2 AS client_ip_2,
    CAST(JSON_VALUE(metadata, "$.client_ip") AS Int64) - 3 AS client_ip_3
ORDER BY c DESC
LIMIT 10;
-- Q36: Сложная фильтрация по датам
SELECT
    url,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.dont_count") AS Int32) = 0
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND JSON_VALUE(metadata, "$.url") <> ''
GROUP BY JSON_VALUE(metadata, "$.url") as url
ORDER BY views DESC
LIMIT 10;
-- Q37: Аналогичный запрос для заголовков
SELECT
    title,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.dont_count") AS Int32) = 0
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND JSON_VALUE(metadata, "$.title") <> ''
GROUP BY JSON_VALUE(metadata, "$.title") as title
ORDER BY views DESC
LIMIT 10;
-- Q38: Сложная фильтрация с отступом
SELECT
    url,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND CAST(JSON_VALUE(metadata, "$.is_link") AS Int32) <> 0
    AND CAST(JSON_VALUE(metadata, "$.is_download") AS Int32) = 0
GROUP BY JSON_VALUE(metadata, "$.url") AS url
ORDER BY views DESC
LIMIT 10
OFFSET 1000;
-- Q39: Сложная группировка с множеством условий
SELECT
    traffic_source_id,
    search_engine_id,
    adv_engine_id,
    src,
    dst,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
GROUP BY
    JSON_VALUE(metadata, "$.traffic_source_id") AS traffic_source_id,
    JSON_VALUE(metadata, "$.search_engine_id") AS search_engine_id,
    JSON_VALUE(metadata, "$.adv_engine_id") AS adv_engine_id,
    CASE
        WHEN JSON_VALUE(metadata, "$.search_engine_id") = '0'
        AND JSON_VALUE(metadata, "$.adv_engine_id") = '0'
        THEN JSON_VALUE(metadata, "$.referer")
        ELSE ''
    END AS src,
    JSON_VALUE(metadata, "$.url") AS dst
ORDER BY views DESC
LIMIT 10
OFFSET 1000;
-- Q40: Группировка по хешу URL
SELECT
    url_hash,
    date,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND JSON_VALUE(metadata, "$.traffic_source_id") IN ('-1', '6')
    AND JSON_VALUE(metadata, "$.referer_hash") = '3594120000172545465'
GROUP BY
    JSON_VALUE(metadata, "$.url_hash") AS url_hash,
    CAST(timestamp AS Date) AS date 
ORDER BY views DESC
LIMIT 10
OFFSET 100;
-- Q41: Сложная группировка по размерам окна
SELECT
    window_client_width,
    window_client_height,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND CAST(JSON_VALUE(metadata, "$.dont_count") AS Int32) = 0
    AND JSON_VALUE(metadata, "$.url_hash") = '2868770270353813622'
GROUP BY
    JSON_VALUE(metadata, "$.window_client_width") AS window_client_width,
    JSON_VALUE(metadata, "$.window_client_height") AS window_client_height
ORDER BY views DESC
LIMIT 10
OFFSET 10000;
-- Q42: Группировка по минутам
SELECT
    M,
    COUNT(*) AS views
FROM `{table}`
WHERE service_name = 'service1'
    AND timestamp BETWEEN (CurrentUtcTimestamp() - Interval("P7D")) AND CurrentUtcTimestamp()
    AND CAST(JSON_VALUE(metadata, "$.is_refresh") AS Int32) = 0
    AND CAST(JSON_VALUE(metadata, "$.dont_count") AS Int32) = 0
GROUP BY CAST(CAST(timestamp AS Uint64) / 60000000 * 60000000 AS DateTime) AS M
ORDER BY M
LIMIT 10
OFFSET 1000;
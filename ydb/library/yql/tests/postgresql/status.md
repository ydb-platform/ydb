==Статус прохождения регрессионных тестов PG в YQL

#|
||№ п/п | Имя теста|Число операторов| Из них выполняется| % выполнения | Последнее обновление | Основные проблемы ||
|| 1 | boolean | 93 | 79 (+6) | 84.95 | 22.01.2024 | YQL-17569 ||
|| 2 | char | 25 | 16 (+13) | 64.0 | 19.01.2024 | YQL-17571 ||
|| 3 | name | 40 | 22 | 55.0 | 29.09.2023 | YQL-17598 ||
|| 4 | varchar | 24 | 15 (+13) | 62.5 | 19.01.2024 | YQL-17603 ||
|| 5 | text | 76 | 16 (+1) | 21.05 | 22.01.2024 | YQL-17605 ||
|| 6 | int2 | 49 | 47 | 95.92 | 29.09.2023 | YQL-17612 ||
|| 7 | int4 | 70 | 70 | 100.0 | 29.09.2023 | ||
|| 8 | int8 | 142 | 134 (+21) | 94.37 | 26.01.2024 | YQL-17614 ||
|| 9 | oid | 27 | 27 (+6) | 100.0 | 22.01.2024 | YQL-17623 ||
|| 10 | float4 | 96 | 82 (+2) | 85.42 | 26.01.2024 | YQL-17586 ||
|| 11 | float8 | 168 | 140 (+8) | 83.33 | 26.01.2024 | YQL-17628 ||
|| 12 | bit | 115 | 84 | 73.04 | 12.12.2023 | YQL-17634 ||
|| 13 | numeric | 915 | 794 (+73) | 86.78 | 26.01.2024 | YQL-17629 ||
|| 14 | uuid | 36 | 0 | 0.0 | 02.05.2023 | YQL-17636 ||
|| 15 | strings | 390 | 103 (+17) | 26.41 | 26.01.2024 | YQL-17587 ||
|| 16 | numerology | 24 | 8 | 33.33 | 26.07.2023 |  ||
|| 17 | date | 264 | 200 | 75.76 | 12.12.2023 | ||
|| 18 | time | 39 | 33 | 84.62 | 12.12.2023 | ||
|| 19 | timetz | 45 | 29 | 64.44 | 12.12.2023 | ||
|| 20 | timestamp | 145 | 98 | 67.59 | 12.12.2023 | ||
|| 21 | timestamptz | 315 | 114 (+6) | 36.19 | 26.01.2024 | ||
|| 22 | interval | 168 | 115 | 68.45 | 25.10.2023 | ||
|| 23 | horology | 306 | 79 | 25.82 | 10.08.2023 | SET, DateStyle, TimeZone, автоматически назначаемые имена колонкам-выражениям, SET TIME ZOME, RESET TIME ZONE, интервальный тип ПГ, ||
|| 24 | comments | 7 | 7 | 100.0 | 25.05.2023 |  ||
|| 25 | expressions | 63 | 14 | 22.22 | 25.10.2023 | ||
|| 26 | unicode | 13 | 4 | 30.77 | 10.08.2023 | ||
|| 27 | create_table | 368 | 43 | 11.68 | 12.12.2023 | CREATE UNLOGGED TABLE, REINDEX, PREPARE ... SELECT, DEALLOCATE, \gexec, pg_class, pg_attribute, CREATE TABLE PARTITION OF ||
|| 28 | insert | 357 | 15 | 4.2 | 12.12.2023 | CREATE TEMP TABLE, ALTER TABLE, DROP TABLE, CREATE TYPE, CREATE RULE, \d+, DROP TYPE, create table...partition by range, create table ... partition of ..., tableoid::regclass, create or replace function, create operator, ||
|| 29 | create_misc | 76 | 3 | 3.95 | 29.09.2023 | ||
|| 30 | select | 88 | 9 | 10.23 | 12.12.2023 | порядок сортировки в виде  ORDER BY поле using > или <, а также NULLS FIRST/LAST; ANALYZE, переменные enable_seqscan, enable_bitmapscan, enable_sort,  whole-row Var referencing a subquery, подзапросы внутри values, INSERT INTO ... DEFAULT VALUES, Range sub select unsupported lateral, CREATE INDEX, DROP INDEX, explain (опции costs, analyze, timing, summary), SELECT 1 AS x ORDER BY x; CREATE FUNCTION, DROP FUNCTION, table inheritance, PARTITION BY ||
|| 31 | select_into | 67 | 3 | 4.48 | 27.07.2023 | ||
|| 32 | select_distinct | 46 | 1 | 2.17 | 27.07.2023 | ||
|| 33 | select_distinct_on | 4 | 0 | 0.0 | 25.05.2023 | ||
|| 34 | select_implicit | 44 | 13 | 29.55 | 12.12.2023 | ||
|| 35 | select_having | 23 | 16 | 69.57 | 12.12.2023 | ||
|| 36 | subselect | 234 | 5 (+3) | 2.14 | 19.01.2024 | YQL-17589 ||
|| 37 | union | 186 | 0 | 0.0 | 25.05.2023 | YQL-17590 ||
|| 38 | case | 63 | 29 | 46.03 | 12.12.2023 | implicit casts, create function volatile ||
|| 39 | join | 591 | 106 | 17.94 | 12.12.2023 | ||
|| 40 | aggregates | 416 | 66 (+15) | 15.87 | 23.01.2024 | YQL-17627 ||
|| 41 | arrays | 410 | 119 | 29.02 | 12.12.2023 | ||
|| 42 | update | 288 | 22 | 7.64 | 12.12.2023 | :-переменные ||
|| 43 | delete | 10 | 5 (+5) | 50.0 | 27.01.2024 | YQL-17585 ||
|| 44 | dbsize | 24 | 24 | 100.0 | 10.08.2023 | ||
|| 45 | window | 298 | 5 | 1.68 | 12.12.2023 | YQL-17592 ||
|| 46 | functional_deps | 40 | 7 (+1) | 17.5 | 19.01.2024 | ||
|| 47 | json | 454 | 113 | 24.89 | 26.01.2024 | ||
|| 48 | jsonb | 1017 | 380 | 37.36 | 26.01.2024 | ||
|| 49 | json_encoding | 42 | 42 | 100.0 | 29.05.2023 | ||
|| 50 | jsonpath | 169 | 152 | 89.94 | 29.05.2023 | числа с точкой без целой части (например .1), литерал '00' ||
|| 51 | jsonpath_encoding | 31 | 31 | 100.0 | 29.05.2023 | ||
|| 52 | jsonb_jsonpath | 427 | 88 | 20.61 | 12.12.2023 | ||
|| 53 | limit | 84 | 5 | 5.95 | 10.08.2023 | ||
|| 54 | truncate | 193 | 33 | 17.1 | 12.12.2023 | ||
|| 55 | alter_table | 1679 | 11 | 0.66 | 12.12.2023 | COMMENT ON TABLE ||
|| 56 | xml | 234 | 15 | 6.41 | 12.12.2023 | \set VERBOSITY ||
|#

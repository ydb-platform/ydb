==Статус прохождения регрессионных тестов PG в YQL==

#|
||№ п/п | Имя теста|Число операторов| Из них выполняется| % выполнения | Последнее обновление | Основные проблемы ||
|| 1 | boolean | 93 | 73 (+6) | 78.49 | 16.12.2023 | DROP TABLE, implicit casts ||
|| 2 | char | 25 | 3 | 12.0 | 25.05.2023 | implicit cast, pgbpchar vs pgtext ||
|| 3 | name | 40 | 22 (+17) | 55.0 | 29.09.2023 | parse_ident, implicit casts ||
|| 4 | varchar | 24 | 2 | 8.33 | 25.05.2023 | ||
|| 5 | text | 76 | 15 (+10) | 19.74 | 12.12.2023 | строковые функции (format, concat, concat_ws, length) и оператор конкатенации, implicit casts, отличаются сообщения об ошибках для оператора конкатенации с аргументами неподходящих типов ||
|| 6 | int2 | 49 | 47 (+39) | 95.92 | 29.09.2023 | ||
|| 7 | int4 | 70 | 70 (+42) | 100.0 | 29.09.2023 | ||
|| 8 | int8 | 142 | 48 (+35) | 33.8 | 12.12.2023 | generate_series, pg_type, gcd, implicit casts ||
|| 9 | oid | 27 | 21 (+19) | 77.78 | 29.09.2023 | ||
|| 10 | float4 | 96 | 48 (+31) | 50.0 | 29.09.2023 | CREATE TYPE, CREATE FUNCTION, WITH, форматирование NaN и Infinity, float4send ||
|| 11 | float8 | 168 | 96 (+1) | 57.14 | 25.10.2023 | CREATE CAST, форматирование NaN и Infinity, extra_float_digits, implicit casts, float8send ||
|| 12 | bit | 115 | 84 (+80) | 73.04 | 12.12.2023 | substring, COPY  FROM stdin, битовые константы ||
|| 13 | numeric | 915 | 715 (+189) | 78.14 | 12.12.2023 | CREATE UNIQUE INDEX, VACUUM ANALYZE, implicit casts, ошибочно проходит cast в int2 и int8, форматирование NaN и Infinity, COPY FROM stdin, SET lc_numeric, умножение больших целых чисел не дает в результате число с плавающей точкой, sum(), округление, nullif, форматирование чисел ||
|| 14 | uuid | 36 | 0 | 0.0 | 02.05.2023 | ||·
|| 15 | strings | 390 | 31 (+3) | 7.95 | 25.08.2023 | SET, RESET, standard_conforming_strings, bytea_output, неинициализированная поддержка регулярок, pg_class  ||
|| 16 | numerology | 24 | 8 (+4) | 33.33 | 26.07.2023 |  ||
|| 17 | date | 264 | 200 (+183) | 75.76 | 12.12.2023 | ||
|| 18 | time | 39 | 33 (+22) | 84.62 | 12.12.2023 | ||
|| 19 | timetz | 45 | 29 (+16) | 64.44 | 12.12.2023 | ||
|| 20 | timestamp | 145 | 98 (+17) | 67.59 | 12.12.2023 | ||
|| 21 | timestamptz | 315 | 108 (+25) | 34.29 | 12.12.2023 | ||
|| 22 | interval | 168 | 115 (+1) | 68.45 | 25.10.2023 | ||
|| 23 | horology | 306 | 79 (+28) | 25.82 | 10.08.2023 | SET, DateStyle, TimeZone, автоматически назначаемые имена колонкам-выражениям, SET TIME ZOME, RESET TIME ZONE, интервальный тип ПГ, ||
|| 24 | comments | 7 | 7 | 100.0 | 25.05.2023 |  ||
|| 25 | expressions | 63 | 14 (+4) | 22.22 | 25.10.2023 | ||
|| 26 | unicode | 13 | 4 (+4) | 30.77 | 10.08.2023 | ||
|| 27 | create_table | 368 | 43 | 11.68 | 12.12.2023 | CREATE UNLOGGED TABLE, REINDEX, PREPARE ... SELECT, DEALLOCATE, \gexec, pg_class, pg_attribute, CREATE TABLE PARTITION OF ||
|| 28 | insert | 357 | 15 (+5) | 4.2 | 12.12.2023 | CREATE TEMP TABLE, ALTER TABLE, DROP TABLE, CREATE TYPE, CREATE RULE, \d+, DROP TYPE, create table...partition by range, create table ... partition of ..., tableoid::regclass, create or replace function, create operator, ||
|| 29 | create_misc | 76 | 3 (+2) | 3.95 | 29.09.2023 | ||
|| 30 | select | 88 | 9 (+5) | 10.23 | 12.12.2023 | порядок сортировки в виде  ORDER BY поле using > или <, а также NULLS FIRST/LAST; ANALYZE, переменные enable_seqscan, enable_bitmapscan, enable_sort,  whole-row Var referencing a subquery, подзапросы внутри values, INSERT INTO ... DEFAULT VALUES, Range sub select unsupported lateral, CREATE INDEX, DROP INDEX, explain (опции costs, analyze, timing, summary), SELECT 1 AS x ORDER BY x; CREATE FUNCTION, DROP FUNCTION, table inheritance, PARTITION BY ||
|| 31 | select_into | 67 | 3 | 4.48 | 27.07.2023 | ||
|| 32 | select_distinct | 46 | 1 | 2.17 | 27.07.2023 | ||
|| 33 | select_distinct_on | 4 | 0 | 0.0 | 25.05.2023 | ||
|| 34 | select_implicit | 44 | 13 (+2) | 29.55 | 12.12.2023 | ||
|| 35 | select_having | 23 | 16 (+5) | 69.57 | 12.12.2023 | ||
|| 36 | subselect | 234 | 2 | 0.85 | 25.05.2023 | ||
|| 37 | union | 186 | 0 | 0.0 | 25.05.2023 | ||
|| 38 | case | 63 | 29 (+16) | 46.03 | 12.12.2023 | implicit casts, create function volatile ||
|| 39 | join | 591 | 106 (+83) | 17.94 | 12.12.2023 | ||
|| 40 | aggregates | 416 | 51 (+50) | 12.26 | 12.12.2023 | ||
|| 41 | arrays | 410 | 119 (+118) | 29.02 | 12.12.2023 | ||
|| 42 | update | 288 | 22 (+5) | 7.64 | 12.12.2023 | :-переменные ||
|| 43 | delete | 10 | 0 | 0.0 | 25.05.2023 | ||
|| 44 | dbsize | 24 | 24 (+16) | 100.0 | 10.08.2023 | ||
|| 45 | window | 298 | 5 (+3) | 1.68 | 12.12.2023 | ||
|| 46 | functional_deps | 40 | 6 (+2) | 15.0 | 12.12.2023 | ||
|| 47 | json | 454 | 115 (+64) | 25.33 | 12.12.2023 | ||
|| 48 | jsonb | 1017 | 382 (+1) | 37.56 | 16.12.2023 | ||
|| 49 | json_encoding | 42 | 42 | 100.0 | 29.05.2023 | ||
|| 50 | jsonpath | 169 | 152 | 89.94 | 29.05.2023 | числа с точкой без целой части (например .1), литерал '00' ||
|| 51 | jsonpath_encoding | 31 | 31 | 100.0 | 29.05.2023 | ||
|| 52 | jsonb_jsonpath | 427 | 88 (+83) | 20.61 | 12.12.2023 | ||
|| 53 | limit | 84 | 5 (+4) | 5.95 | 10.08.2023 | ||
|| 54 | truncate | 193 | 33 (+30) | 17.1 | 12.12.2023 | ||
|| 55 | alter_table | 1679 | 11 (+7) | 0.66 | 12.12.2023 | COMMENT ON TABLE ||
|| 56 | xml | 234 | 15 (+12) | 6.41 | 12.12.2023 | \set VERBOSITY ||
|#

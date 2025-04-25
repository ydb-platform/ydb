PRAGMA OptimizeSimpleILIKE;

SELECT
    'test' ILIKE 'test',
    'TEST' ILIKE 'test',
    'тест' ILIKE 'ТЕСТ',
    'Привет' ILIKE 'пРиВеТ',
    'prefix' ILIKE 'Pre%',
    'префикс' ILIKE 'Пре%',
    'suFfix' ILIKE '%Fix',
    'СУФФИКС' ILIKE '%Икс',
    'инфикс' ILIKE '%Фи%',
    'Комплекс' ILIKE '%О%П%С',

    --negative
    'тест' ILIKE 'Тесть',
    'Тост' ILIKE '%мост',
    'Лось' ILIKE 'Лом%'
;

SELECT
    NULL ILIKE 'test',
    NULL ILIKE 'te%',
    NULL ILIKE '%st',
    NULL ILIKE '%es%'
;

$table = [
    <|
        str: 'TeSt',
        optStr: Just('TeSt'),
        nullStr: Nothing(String?)
    |>
];

SELECT
    str ILIKE 'test',
    str ILIKE 'te%',
    str ILIKE '%st',
    str ILIKE '%es%',
    optStr ILIKE 'test',
    optStr ILIKE 'te%',
    optStr ILIKE '%st',
    optStr ILIKE '%es%',
    nullStr ILIKE 'test',
    nullStr ILIKE 'te%',
    nullStr ILIKE '%st',
    nullStr ILIKE '%es%'
FROM
    AS_TABLE($table)
;

# Traceability Matrix

### TTL
#### ISSUE-13526
Description: Test Suite: cs/tiering+ttl

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13468 | Test tiering. Functional. Data deleted by DELETE statement are deleted from S3 | При явном удалении данные с помощью DELETE связанные с ним данные удаляются из S3 (тест дожидается пропажи данных в S3)

Сценарий:
1 создаются нужные ресурсы:
1.1 таблица

ts Timestamp,
s String,
val Int64

1.2 два бакета в s3 (или очищаются ранее созданные)

aws --profile=default --endpoint-url=http://127.0.0.1:12012 --region=us-east-1 s3 mb s3://s3_cold
aws --profile=default --endpoint-url=http://127.0.0.1:12012 --region=us-east-1 s3 mb s3://s3_frozen

1.3 два EXTERNAL DATA SOURCE:
s3_cold
s3_frozen

CREATE OBJECT access_key (TYPE SECRET) WITH (value="")
CREATE OBJECT secret_key (TYPE SECRET) WITH (value="")

create external data source s3_cold WITH(source_type="ObjectStorage", LOCATION="http://127.0.0.1:12012", AUTH_METHOD="AWS", AWS_ACCESS_KEY_ID_SECRET_NAME="access_key", AWS_REGION="ru-central1", AWS_SECRET_ACCESS_KEY_SECRET_NAME="secret_key")
повторить для s3_frozen вместо s3_cold

    Таблица заполняется данными равномерно распределёнными по ts в интервале 2010-2030 год
    смотрим объём данный в .sys
    Настраивается тиринг в два тира:

ALTER TABLE mytable SET (
     TTL =
     Interval("P100D") TO EXTERNAL DATA SOURCE `/Root/test/s3_cold`,
     Interval("P3000D") TO EXTERNAL DATA SOURCE `/Root/test/s3_frozen`
     ON ts
     );


Контролируем по .sys что данные разъехались по тирам пропорционально длительности интервалов
Контролируем объём данных в бакетах s3

Выполняем команду DELETE FROM `mytable`
Проверяем по .sys, что данные удалились из всех тиров
Проверяем, что все данные удалились из обоих бакетов
 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13468)](https://github.com/ydb-platform/ydb/issues/13468) | Pending |
| #13467 | Test tiering. Functional. When configuring DELETE tier, data evaporates from S3 | Изменение настроек тиринга в части удаления данных из S3 приводит к полной очистке бакета 

Сценарий:
1 создаются нужные ресурсы:
1.1 таблица
```
ts Timestamp,
s String,
val Int64
```

1.2 два бакета в s3 (или очищаются ранее созданные)
1.3 два EXTERNAL DATA SOURCE:
s3_cold
s3_frozen

2. Таблица заполняется данными равномерно распределёнными по ts в интервале 2010-2030 год
смотрим объём данный в .sys
3. Настраивается тиринг в два тира:
```
ALTER TABLE `mytable` SET (
    TTL =
        DateTime::IntervalFromDays(1000) TO EXTERNAL DATA SOURCE `/Root/s3_cold`,
        DateTime::IntervalFromDays(3000) TO EXTERNAL DATA SOURCE `/Root/s3_frozen`
    ON ts
);
```
Контролируем по .sys что данные разъехались по тирам пропорционально длительности интервалов
Контролируем объём данных в бакетах s3

4. Меняем правила тиринга
```
ALTER TABLE `mytable` SET (
    TTL =
        DateTime::IntervalFromDays(1000)
    ON ts
);

Проверяем по .sys, что данные удалились из всех тиров, кроме __DEFAULT
Проверяем, что все данные удалились из обоих бакетов
 
 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13467)](https://github.com/ydb-platform/ydb/issues/13467) | Pending |
| #13466 | Test tiering. Functional. Check data migration when altering tiering settings | Изменение настроек тиринга приводит к ожидаемому перемещению данных из одного тира в другой 
Подробное описание:
1. Создать S3 и создать два `EXTERNAL DATA SOURCE` для двух разных бакета (`bucket1` и `bucket2`)
2. Создать табличку.
```
CREATE TABLE `table_name` (
    timestamp Timestamp,
    value Uint64,
    data String,
    PRIMARY KEY(timestamp)
)
WITH (
    STORE = COLUMN,
);
```
3. Заполнить данные в табличку.
4. Изменить `TTL`, так чтобы данные вытиснялись во второй бакет
```
ALTER TABLE `table_name` SET (
    TTL = INTERVAL("PT2M") TO EXTERNAL DATA SOURCE `/Root/bucket1`,
    ON timestamp
);
```
5. Дождаться вытиснения и проверить размер первого бакета (размер 1-го бакета должен совпадать с количеством вставленных данных, а 2-й бакет должен быть пустым)
6. Изменить `TTL`, так чтобы данные вытиснялись во второй бакет.
```
ALTER TABLE `table_name` SET (
    TTL = INTERVAL("PT5M") TO EXTERNAL DATA SOURCE `/Root/bucket2`,
    ON timestamp
);
```
7. Дождаться вытиснения и проверить размеры бакетов (размер 2-го бакета должен совпадать с количеством вставленных данных, а 1-й бакет должен быть пустым)
8. Изменить `TTL`, так чтобы данные вернулись в `BlobStorage`
```
ALTER TABLE `table_name` SET (
    TTL = INTERVAL("P10000D") TO EXTERNAL DATA SOURCE `/Root/bucket1`,
    ON timestamp
);
```
9. Дождаться возвращения данных в `BlobStorage` и проверить размеры бакетов (размер 1-го и 2-го бакета должны быть пустым) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13466)](https://github.com/ydb-platform/ydb/issues/13466) | Pending |
| #13465 | Test tiering. Functional. Check data correctness | Выполняется большое число записи, удаления, модификации большого числа данных с тем, чтобы все данные были вытеснены. Сравниваются прочитанные данные и ожидаемые
TBD Подробное описание теста | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13465)](https://github.com/ydb-platform/ydb/issues/13465) | Pending |
| #13542 | Test tiering. Functional. Check data availability and correctness while changing ttl settings | Таблица наполняется данными, настройки тиринга меняются постоянно, проверяется, что все время считываются корректные данные приоритет 1
3 тира и данные записаны в соответсвии с паттерном логов, т.е. каждая запись содержит строки локализованные по времени | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13542)](https://github.com/ydb-platform/ydb/issues/13542) | Pending |
| #13543 | Test. sys reflects data distribution across tiers while modifying data | Выполняется большое число точечных модификаций данных. В sysview отражается статус отставания вытеснения данных | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13543)](https://github.com/ydb-platform/ydb/issues/13543) | Pending |
| #13544 | Test tiering. Stress. Ustable network connection | Протетсировать работу тиринга с нестабильным доступом до s3 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13544)](https://github.com/ydb-platform/ydb/issues/13544) | Pending |
| #13545 | Test tiering. Stability. Temporary unavailable s3 | Временно потеряно соединение с s3. Ожидаемое поведение - после возобновления связи (через какое время?) перенос данных возобновляется. На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13545)](https://github.com/ydb-platform/ydb/issues/13545) | Pending |
| #13546 | Test tiering. Stability. Writing when blobstorage is full | Постоянно потеряно соединение с S3, места на диске не хватает. Ожидаемое поведение - сообщение об ошибке записи пользователю. . На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13546)](https://github.com/ydb-platform/ydb/issues/13546) | Pending |
| #13619 | Test tiering. Add column works for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, добавляются новые поля. Ожидаемое поведение - система на всей глубине хранения отображает новые поля | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13619)](https://github.com/ydb-platform/ydb/issues/13619) | Pending |
| #13620 | Test teiring. Drop Column works well for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, удаляются существующие not null поля. Ожидаемое поведение - система на всей глубине хранения выполняет запросы  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13620)](https://github.com/ydb-platform/ydb/issues/13620) | Pending |
| #13621 | Test tiering. Alter column works well for offloaded data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13621)](https://github.com/ydb-platform/ydb/issues/13621) | Pending |


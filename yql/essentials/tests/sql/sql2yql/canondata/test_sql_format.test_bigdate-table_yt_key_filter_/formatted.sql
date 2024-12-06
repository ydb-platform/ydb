/* postgres can not */
/* multirun can not */
USE plato;
PRAGMA yt.UseNewPredicateExtraction;

INSERT INTO OutDate32
SELECT
    *
FROM
    as_table(AsList(
        <|key: Date32('-144169-1-1')|>,
        <|key: Date32('148107-12-31')|>
    ))
ORDER BY
    key
;

INSERT INTO OutDatetime64
SELECT
    *
FROM
    as_table(AsList(
        <|key: Datetime64('-144169-1-1T0:0:0Z')|>,
        <|key: Datetime64('148107-12-31T23:59:59Z')|>
    ))
ORDER BY
    key
;

INSERT INTO OutTimestamp64
SELECT
    *
FROM
    as_table(AsList(
        <|key: Timestamp64('-144169-1-1T0:0:0Z')|>,
        <|key: Timestamp64('148107-12-31T23:59:59.999999Z')|>
    ))
ORDER BY
    key
;
COMMIT;

SELECT
    *
FROM
    OutDate32
WHERE
    key > Date('1970-1-1')
    AND key > Datetime('1970-1-1T0:0:0Z')
    AND key > Timestamp('1970-1-1T0:0:0Z')
    AND key > Date32('-144169-1-1')
    AND key > Datetime64('-144169-1-1T0:0:0Z')
    AND key > Timestamp64('-144169-1-1T0:0:0Z')
    AND key >= Date('2105-12-31')
    AND key >= Datetime('2105-12-31T23:59:59Z')
    AND key >= Timestamp('2105-12-31T23:59:59Z')
    AND key >= Date32('148107-12-31')
    AND key >= Datetime64('148107-12-31T0:0:0Z')
    AND key >= Timestamp64('148107-12-31T0:0:0Z')
;

SELECT
    *
FROM
    OutDate32
WHERE
    key < Date('2105-12-31')
    AND key < Datetime('2105-12-31T23:59:59Z')
    AND key < Timestamp('2105-12-31T23:59:59.999999Z')
    AND key < Date32('148107-12-31')
    AND key < Datetime64('148107-12-31T23:59:59Z')
    AND key < Timestamp64('148107-12-31T23:59:59.999999Z')
    AND key <= Date('1970-1-1')
    AND key <= Datetime('1970-1-1T0:0:0Z')
    AND key <= Timestamp('1970-1-1T0:0:0Z')
    AND key <= Date32('-144169-1-1')
    AND key <= Datetime64('-144169-1-1T0:0:0Z')
    AND key <= Timestamp64('-144169-1-1T0:0:0Z')
;

SELECT
    *
FROM
    OutDatetime64
WHERE
    key > Date('1970-1-1')
    AND key > Datetime('1970-1-1T0:0:0Z')
    AND key > Timestamp('1970-1-1T0:0:0Z')
    AND key > Date32('-144169-1-1')
    AND key > Datetime64('-144169-1-1T0:0:0Z')
    AND key > Timestamp64('-144169-1-1T0:0:0Z')
    AND key >= Date('2105-12-31')
    AND key >= Datetime('2105-12-31T23:59:59Z')
    AND key >= Timestamp('2105-12-31T23:59:59Z')
    AND key >= Date32('148107-12-31')
    AND key >= Datetime64('148107-12-31T0:0:0Z')
    AND key >= Timestamp64('148107-12-31T0:0:0Z')
;

SELECT
    *
FROM
    OutDatetime64
WHERE
    key < Date('2105-12-31')
    AND key < Datetime('2105-12-31T23:59:59Z')
    AND key < Timestamp('2105-12-31T23:59:59.999999Z')
    AND key < Date32('148107-12-31')
    AND key < Datetime64('148107-12-31T23:59:59Z')
    AND key < Timestamp64('148107-12-31T23:59:59.999999Z')
    AND key <= Date('1970-1-1')
    AND key <= Datetime('1970-1-1T0:0:0Z')
    AND key <= Timestamp('1970-1-1T0:0:0Z')
    AND key <= Date32('-144169-1-1')
    AND key <= Datetime64('-144169-1-1T0:0:0Z')
    AND key <= Timestamp64('-144169-1-1T0:0:0Z')
;

SELECT
    *
FROM
    OutTimestamp64
WHERE
    key > Date('1970-1-1')
    AND key > Datetime('1970-1-1T0:0:0Z')
    AND key > Timestamp('1970-1-1T0:0:0Z')
    AND key > Date32('-144169-1-1')
    AND key > Datetime64('-144169-1-1T0:0:0Z')
    AND key > Timestamp64('-144169-1-1T0:0:0Z')
    AND key >= Date('2105-12-31')
    AND key >= Datetime('2105-12-31T23:59:59Z')
    AND key >= Timestamp('2105-12-31T23:59:59Z')
    AND key >= Date32('148107-12-31')
    AND key >= Datetime64('148107-12-31T0:0:0Z')
    AND key >= Timestamp64('148107-12-31T0:0:0Z')
;

SELECT
    *
FROM
    OutTimestamp64
WHERE
    key < Date('2105-12-31')
    AND key < Datetime('2105-12-31T23:59:59Z')
    AND key < Timestamp('2105-12-31T23:59:59.999999Z')
    AND key < Date32('148107-12-31')
    AND key < Datetime64('148107-12-31T23:59:59Z')
    AND key < Timestamp64('148107-12-31T23:59:59.999999Z')
    AND key <= Date('1970-1-1')
    AND key <= Datetime('1970-1-1T0:0:0Z')
    AND key <= Timestamp('1970-1-1T0:0:0Z')
    AND key <= Date32('-144169-1-1')
    AND key <= Datetime64('-144169-1-1T0:0:0Z')
    AND key <= Timestamp64('-144169-1-1T0:0:0Z')
;

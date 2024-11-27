/* syntax version 1 */
$zero = unwrap(CAST(0 AS Interval));
$one = unwrap(CAST(1 AS Interval));

-- we want to check both optional<interval> and plain interval
$prepared =
    SELECT
        CAST(key AS Interval) ?? $zero AS interval_data
    FROM plato.Input;

$source =
    SELECT
        interval_data,
        interval_data + $one AS interval_data2,
        just(interval_data) AS optional_interval_data
    FROM $prepared;

-- percentile factory can work with plain number and with tuple of numbers.
-- to achive second call we must make several percentile invocations with
-- same column name
$data_plain =
    SELECT
        percentile(interval_data, 0.8) AS result
    FROM $source;

-- optimization should unite this into one call to percentile with tuple as argument
$data_tuple =
    SELECT
        percentile(interval_data2, 0.8) AS result_1,
        percentile(interval_data2, 0.6) AS result_2
    FROM $source;

$data_optional =
    SELECT
        percentile(optional_interval_data, 0.4) AS result
    FROM $source;

SELECT
    EnsureType(result, Interval?)
FROM $data_plain;

SELECT
    EnsureType(result_1, Interval?)
FROM $data_tuple;

SELECT
    EnsureType(result_2, Interval?)
FROM $data_tuple;

SELECT
    result
FROM $data_optional;

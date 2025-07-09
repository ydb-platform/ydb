USE plato;

$currentdate = CAST("2025-01-01" AS date);


$Q =
    (
        SELECT
            (
                CASE 
                    WHEN Number_mounth IN (1,2,3) THEN "Q1"
                    WHEN Number_mounth IN (4,5,6) THEN "Q2"
                    WHEN Number_mounth IN (7,8,9) THEN "Q3"
                    WHEN Number_mounth IN (10,11,12) THEN "Q4"
                    ELSE "NULL"
                END
            ) AS quarter,
            Number_mounth
          
        FROM
            (
                SELECT
                [1,2,3,4,5,6,7,8,9,10,11,12] AS Number_mounth,

            )
        FLATTEN BY Number_mounth
    );
$current =
    (
        SELECT
            Data.start_quarter AS start_quarter,
            Data.end_quarter AS end_quarter,

            DateTime::ToDays($currentdate - start_quarter) AS number_day_quarter
        FROM 
            (
                SELECT
                    DateTime::MakeDate(DateTime::StartOfMonth($currentdate - DateTime::IntervalFromSeconds(6048000))) AS start_quarter,
                    
                    $currentdate - DateTime::IntervalFromSeconds(72000) AS end_quarter,
                    DateTime::GetMonth($currentdate - DateTime::IntervalFromSeconds(72000)) AS Month
            ) AS Data
        LEFT JOIN $Q AS Q ON CAST(Data.Month as Int32) = Q.Number_mounth 
    );
    
$start_quarter = 
    (
        SELECT
            start_quarter
        FROM $current
    );
$end_quarter = 
    (
        SELECT
            end_quarter
        FROM $current
    );

$number_day_quarter = 
    (
        SELECT
            number_day_quarter
        FROM $current
    );
$consumption = 
    (
        SELECT
            DateTime::MakeDate(DateTime::ParseIso8601(date_1)) as date_1,
            id_1,

        FROM Input1
        WHERE DateTime::MakeDate(DateTime::ParseIso8601(date_1)) >= $start_quarter 
            AND DateTime::MakeDate(DateTime::ParseIso8601(date_1)) <= $end_quarter
        
    );

$instance = 
    (
        SELECT
            id_2
        FROM
            Input2
        WHERE
            DateTime::MakeDate(DateTime::ParseIso8601(date_2)) >= $end_quarter
            AND   id_2 = "aaa"
    );

SELECT
    instance.id_2 AS id_2,
    $number_day_quarter AS quarter

FROM $instance AS instance

LEFT JOIN $consumption AS consumption
    ON instance.id_2 = consumption.id_1

GROUP BY 
    instance.id_2;

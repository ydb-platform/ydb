/* postgres can not */
USE plato;

$data = (
    SELECT
        Math::Pi() * CAST(subkey AS Double) AS rad
    FROM Input
);

--INSERT INTO Output
SELECT Math::Sqrt(Math::Sin(rad) * Math::Sin(rad) + Math::Cos(rad) * Math::Cos(rad)) FROM $data

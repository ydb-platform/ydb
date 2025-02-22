USE plato;

SELECT
    a.ni + b.ni,
    a.ni + b.wi,
    a.wi + b.ni,
    a.wi + b.wi
FROM Dates AS a CROSS JOIN Dates as b



USE plato;

SELECT
    a.na + b.ni,
    a.na + b.wi,
    a.naz + b.ni,
    a.naz + b.wi,
    a.nd + b.ni,
    a.nd + b.wi,
    a.ndz + b.ni,
    a.ndz + b.wi,
    a.nt + b.ni,
    a.nt + b.wi,
    a.ntz + b.ni,
    a.ntz + b.wi
FROM Dates as a CROSS JOIN Dates as b



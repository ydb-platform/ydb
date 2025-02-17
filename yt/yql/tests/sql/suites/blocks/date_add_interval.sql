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
    a.ntz + b.wi,

    a.wa + b.ni,
    a.wa + b.wi,
    a.waz + b.ni,
    a.waz + b.wi,
    a.wd + b.ni,
    a.wd + b.wi,
    a.wdz + b.ni,
    a.wdz + b.wi,
    a.wt + b.ni,
    a.wt + b.wi,
    a.wtz + b.ni,
    a.wtz + b.wi,
FROM Dates as a CROSS JOIN Dates as b



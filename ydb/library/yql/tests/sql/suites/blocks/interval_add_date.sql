USE plato;

SELECT
    b.ni + a.na,
    b.wi + a.na,
    b.ni + a.naz,
    b.wi + a.naz,
    b.ni + a.nd,
    b.wi + a.nd,
    b.ni + a.ndz,
    b.wi + a.ndz,
    b.ni + a.nt,
    b.wi + a.nt,
    b.ni + a.ntz,
    b.wi + a.ntz,

    b.ni + a.wa,
    b.wi + a.wa,
    b.ni + a.waz,
    b.wi + a.waz,
    b.ni + a.wd,
    b.wi + a.wd,
    b.ni + a.wdz,
    b.wi + a.wdz,
    b.ni + a.wt,
    b.wi + a.wt,
    b.ni + a.wtz,
    b.wi + a.wtz
FROM Dates as a CROSS JOIN Dates as b



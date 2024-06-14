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
    b.wi + a.ntz
FROM Dates as a CROSS JOIN Dates as b



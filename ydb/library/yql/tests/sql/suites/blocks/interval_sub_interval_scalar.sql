USE plato;

SELECT
    ni - Interval("P1D"),
    wi - Interval("P1D"),
    ni - Interval64("P1D"),
    wi - Interval64("P1D")
FROM Dates;

SELECT
    Interval("P1D") - ni,
    Interval("P1D") - wi,
    Interval64("P1D") - ni,
    Interval64("P1D") - wi
FROM Dates;


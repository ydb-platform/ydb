USE plato;

SELECT
    1ul - 1
; -- warn

SELECT
    1u - 1
; -- warn

SELECT
    1l - 1u
; -- ok

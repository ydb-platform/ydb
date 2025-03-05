USE plato;

SELECT
    ToPg(i8), ToPg(ui8),
    ToPg(i16), ToPg(ui16), 
    ToPg(i32), ToPg(ui32),
    ToPg(i64), ToPg(ui64),
    ToPg(f4), ToPg(f8)
FROM Input


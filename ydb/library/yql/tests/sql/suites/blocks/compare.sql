select
    i8,
    i16     == i32,
    i32     != ui8,
    ui16    <  i64,
    i8      <= ui8,
    ui32    >  i64,
    ui64    >= i16,

    i16     == i32opt,
    i32opt  != ui8,
    ui16    <  i64opt,
    i8opt   <= ui8opt,
    ui32opt >  i64,
    ui64    >= i16,

    i16     == 6u,
    i32opt  != 5,
    7       <  i64opt,
    1/0     <= ui8opt,
    0       >  i64,
    ui64    >= 8u,
from plato.Input
order by i8;

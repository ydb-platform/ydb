USE plato;

SELECT
    ni * Int8("10"),
    wi * Int8("10"),
    ni * Uint8("11"),
    wi * Uint8("11"),
    ni * Int16("12"),
    wi * Int16("12"),
    ni * Uint16("13"),
    wi * Uint16("13"),
    ni * Int32("14"),
    wi * Int32("14"),
    ni * Uint32("15"),
    wi * Uint32("15"),
    ni * Int64("16"),
    wi * Int64("16"),
    ni * Uint64("17"),
    wi * Uint64("17"),
FROM Dates;

SELECT
    Int8("10") * ni,
    Int8("10") * wi,
    Uint8("11") * ni,
    Uint8("11") * wi,
    Int16("12") * ni,
    Int16("12") * wi,
    Uint16("13") * ni,
    Uint16("13") * wi,
    Int32("14") * ni,
    Int32("14") * wi,
    Uint32("15") * ni,
    Uint32("15") * wi,
    Int64("16") * ni,
    Int64("16") * wi,
    Uint64("17") * ni,
    Uint64("17") * wi
FROM Dates;

SELECT 
   Interval("P1D") * i8,
   Interval64("P1D") * i8,
   Interval("P1D") * u8,
   Interval64("P1D") * u8,
   Interval("P1D") * i16,
   Interval64("P1D") * i16,
   Interval("P1D") * u16,
   Interval64("P1D") * u16,
   Interval("P1D") * i32,
   Interval64("P1D") * i32,
   Interval("P1D") * u32,
   Interval64("P1D") * u32,
   Interval("P1D") * i64,
   Interval64("P1D") * i64,
   Interval("P1D") * u64,
   Interval64("P1D") * u64
FROM Dates;

SELECT 
   i8 * Interval("P1D"),
   i8 * Interval64("P1D"),
   u8 * Interval("P1D"),
   u8 * Interval64("P1D"),
   i16 * Interval("P1D"),
   i16 * Interval64("P1D"),
   u16 * Interval("P1D"),
   u16 * Interval64("P1D"),
   i32 * Interval("P1D"),
   i32 * Interval64("P1D"),
   u32 * Interval("P1D"),
   u32 * Interval64("P1D"),
   i64 * Interval("P1D"),
   i64 * Interval64("P1D"),
   u64 * Interval("P1D"),
   u64 * Interval64("P1D")
FROM Dates;



pragma warning("disable","4510");

select bitcast(Yql::Date32(AsAtom("1")) as Uint8)
, 4, bitcast(Yql::Date32(AsAtom("1")) as Int8)
, 5, bitcast(Yql::Date32(AsAtom("1")) as Uint16)
, 6, bitcast(Yql::Date32(AsAtom("1")) as Int16)
, 7, bitcast(Yql::Date32(AsAtom("1")) as Uint32)
, 8, bitcast(Yql::Date32(AsAtom("1")) as Int32)
, 9, bitcast(Yql::Date32(AsAtom("1")) as Uint64)
, 10, bitcast(Yql::Date32(AsAtom("1")) as Int64);

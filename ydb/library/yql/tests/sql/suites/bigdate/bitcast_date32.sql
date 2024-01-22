pragma warning("disable","4510");

select bitcast(Yql::Date32(AsAtom("1")) as Uint8)
, bitcast(Yql::Date32(AsAtom("1")) as Int8)
, bitcast(Yql::Date32(AsAtom("1")) as Uint16)
, bitcast(Yql::Date32(AsAtom("1")) as Int16)
, bitcast(Yql::Date32(AsAtom("1")) as Uint32)
, bitcast(Yql::Date32(AsAtom("1")) as Int32)
, bitcast(Yql::Date32(AsAtom("1")) as Uint64)
, bitcast(Yql::Date32(AsAtom("1")) as Int64);

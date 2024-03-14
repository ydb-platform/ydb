pragma warning("disable","4510");

select bitcast(Yql::Datetime64(AsAtom("1")) as Uint8)
, bitcast(Yql::Datetime64(AsAtom("1")) as Int8)
, bitcast(Yql::Datetime64(AsAtom("1")) as Uint16)
, bitcast(Yql::Datetime64(AsAtom("1")) as Int16)
, bitcast(Yql::Datetime64(AsAtom("1")) as Uint32)
, bitcast(Yql::Datetime64(AsAtom("1")) as Int32)
, bitcast(Yql::Datetime64(AsAtom("1")) as Uint64)
, bitcast(Yql::Datetime64(AsAtom("1")) as Int64);

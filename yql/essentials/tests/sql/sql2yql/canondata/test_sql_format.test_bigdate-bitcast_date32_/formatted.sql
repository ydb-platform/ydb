PRAGMA warning('disable', '4510');

SELECT
    BITCAST(Yql::Date32(AsAtom('1')) AS Uint8),
    4,
    BITCAST(Yql::Date32(AsAtom('1')) AS Int8),
    5,
    BITCAST(Yql::Date32(AsAtom('1')) AS Uint16),
    6,
    BITCAST(Yql::Date32(AsAtom('1')) AS Int16),
    7,
    BITCAST(Yql::Date32(AsAtom('1')) AS Uint32),
    8,
    BITCAST(Yql::Date32(AsAtom('1')) AS Int32),
    9,
    BITCAST(Yql::Date32(AsAtom('1')) AS Uint64),
    10,
    BITCAST(Yql::Date32(AsAtom('1')) AS Int64)
;

PRAGMA warning('disable', '4510');

SELECT
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Uint8),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Int8),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Uint16),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Int16),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Uint32),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Int32),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Uint64),
    BITCAST(Yql::Timestamp64(AsAtom('1')) AS Int64)
;

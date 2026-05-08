PRAGMA warning('error', '*');

SELECT
    Opaque(1ul) + 1,
    1 + Opaque(1ul),
    Opaque(1ul) + Just(1),
    Just(1) + Opaque(1ul)
;

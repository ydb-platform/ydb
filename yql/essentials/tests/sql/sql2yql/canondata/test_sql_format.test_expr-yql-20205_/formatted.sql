/* custom error: Expected type kind: Struct, but got: Optional */
SELECT
    ChooseMembers(Opaque(Just(Just(AsStruct(1 AS x)))), ['x'])
;

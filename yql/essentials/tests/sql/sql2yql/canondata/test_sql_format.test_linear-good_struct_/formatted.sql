$x = MutDictHasItems(MutDictCreate(Int32, String, 0));

SELECT
    FromMutDict(ExpandStruct(WithSideEffects(Opaque(<|z: Void()|>)), $x.0 AS a, $x.1 AS b).a)
;

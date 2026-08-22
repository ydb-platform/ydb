/* custom error: Linear types can be used */
$x = MutDictHasItems(MutDictCreate(Int32, String, 0));

SELECT
    FromMutDict(<|p: ExpandStruct(WithSideEffects(Opaque(<|z: Void()|>)), $x.0 AS a, $x.1 AS b)|>.p.a)
;

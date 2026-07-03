$vt = Variant<foo: Struct<a: Int32>, bar: Int32>;

SELECT
    Variant(<|a: 1|>, 'foo', $vt) == AsVariant(<||>, 'foo')
;

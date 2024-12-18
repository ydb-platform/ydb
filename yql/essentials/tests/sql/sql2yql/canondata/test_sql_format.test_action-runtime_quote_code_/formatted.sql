/* postgres can not */
/* syntax version 1 */
SELECT
    FormatCode(QuoteCode(AsAtom('foo'))),
    FormatCode(QuoteCode(AsTuple())),
    FormatCode(QuoteCode(AsTuple(AsAtom('foo'), AsAtom('bar')))),
    FormatCode(QuoteCode(1)),
    FormatCode(
        QuoteCode(
            ($x, $y) -> {
                RETURN $x + $y;
            }
        )
    ),
    ListMap(
        ListFromRange(1, 4), ($x) -> {
            RETURN FormatCode(
                QuoteCode(
                    ($y) -> {
                        RETURN $x + $y;
                    }
                )
            );
        }
    )
;

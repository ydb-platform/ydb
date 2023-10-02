/* postgres can not */
/* syntax version 1 */
select
    FormatCode(QuoteCode(AsAtom("foo"))),
    FormatCode(QuoteCode(AsTuple())),
    FormatCode(QuoteCode(AsTuple(AsAtom("foo"),AsAtom("bar")))),
    FormatCode(QuoteCode(1)),
    FormatCode(QuoteCode(($x,$y)->{ return $x+$y })),
    ListMap(ListFromRange(1,4), ($x)->{
        return FormatCode(QuoteCode(
            ($y)->{ return $x+$y }
        ))
    });

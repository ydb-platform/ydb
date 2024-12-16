/* postgres can not */
SELECT
    AsTuple(
        AsDict(AsTuple(1, 2u)) == AsDict(AsTuple(1, 2)),
        AsDict(AsTuple(1, 2u)) == AsDict(AsTuple(1, 3)),
        AsDict(AsTuple(1, 2u)) == AsDict(AsTuple(1, 2), AsTuple(3, 4)),
        AsDict(AsTuple(1, 2u)) == AsDict(AsTuple(2, 2)),
        AsDict(AsTuple(1u, 2l)) == AsDict(AsTuple(1u, just(2u))),
        AsDict(AsTuple(1, 2u)) == AsDict(AsTuple(1, 2 / 0)),
    )
;

SELECT
    AsTuple(
        AsDict(AsTuple(1, 2u)) != AsDict(AsTuple(1, 2)),
        AsDict(AsTuple(1, 2u)) != AsDict(AsTuple(1, 3)),
        AsDict(AsTuple(1, 2u)) != AsDict(AsTuple(1, 2), AsTuple(3, 4)),
        AsDict(AsTuple(1, 2u)) != AsDict(AsTuple(2, 2)),
        AsDict(AsTuple(1u, 2l)) != AsDict(AsTuple(1u, just(2u))),
        AsDict(AsTuple(1, 2u)) != AsDict(AsTuple(1, 2 / 0)),
    )
;

$data = [
<|dt:1688910000, event:"A"|>,
<|dt:1688910100, event:"B"|>,
<|dt:1688910200, event:"C"|>,
<|dt:1688910300, event:"A"|>,
<|dt:1688910400, event:"C"|>,
<|dt:1688910500, event:"D"|>,
<|dt:1688910500, event:"C"|>,
<|dt:1688910600, event:"B"|>,
<|dt:1688910800, event:"A"|>,
<|dt:1688910900, event:"C"|>,
<|dt:1688911000, event:"B"|>,
];

pragma FeatureR010="prototype";

SELECT *
FROM AS_TABLE($data) MATCH_RECOGNIZE(
    ORDER BY CAST(dt as Timestamp)
    MEASURES
        FIRST(A.dt) as a,
        FIRST(B.dt) as b,
        FIRST(C.dt) as c
    ONE ROW PER MATCH
    PATTERN (
      PERMUTE(A, B, C)
    )
    DEFINE
        A as A.event = "A",
        B as B.event = "B",
        C as C.event = "C"
    ) AS MATCHED
;


USE plato;
pragma yt.DisableOptimizers="OutHorizontalJoin,HorizontalJoin,MultiHorizontalJoin";

SELECT
    ToPg(s), ToPg(u),
    ToPg(y), ToPg(j),
    ToPg(jd)
FROM Input;


SELECT
    ToPg(tzd),
    ToPg(tzdt), ToPg(tzts),
    ToPg(ud)
FROM Input;

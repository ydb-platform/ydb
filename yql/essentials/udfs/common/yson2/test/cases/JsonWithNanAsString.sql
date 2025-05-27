$src =  Yson::From(0./0.);  --  nan
$src1 = Yson::From(1./0.);  --  inf
$src2 = Yson::From(-1./0.); -- -inf

SELECT 
    Yson::SerializeJson($src,  true AS WriteNanAsString),
    Yson::SerializeJson($src1, true AS WriteNanAsString),
    Yson::SerializeJson($src2, true AS WriteNanAsString)
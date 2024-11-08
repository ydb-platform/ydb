$s = <|
    double1: 1.0000000001,
    double2: 1.000000001,
    double3: 1000000000.5,
    double4: 10000000005.0,
    double5: 10000000000.5,
    double6: 100000000005.0,
|>;

SELECT
    Yson::Serialize(Yson::From($s)),
    Yson::SerializeJson(Yson::From($s))
;
$a1 = Yson::Parse(Yson("{a=1;b=2}"));
$a2 = Yson::Parse(Yson("{a=1;b=2;}"));
$a3 = Yson::Parse(Yson("{b=2;a=1}"));
$b = Yson::Parse(Yson("#"));
$c = Yson::Parse(Yson("{a=1;b=3}"));
$d = Yson::Parse(Yson("{a=#}"));
$e = Yson::Parse(Yson("[a;1;b;2]"));
$f = Yson::Parse(Yson("{a=1u;b=2}"));
$g = Yson::Parse(Yson("{a=1;b=\"2\"}"));
$h = Yson::Parse(Yson("<foo=bar>{a=1;b=2}"));
$i = Yson::Parse(Yson("{a=1;b=<foo=bar>2}"));
$j = Yson::Parse(Yson("[1;a;b;2]"));

SELECT
    Yson::GetHash($a1) AS a1,
    Yson::GetHash($a2) AS a2,
    Yson::GetHash($a3) AS a3,
    Yson::GetHash($b) AS b,
    Yson::GetHash($c) AS c,
    Yson::GetHash($d) AS d,
    Yson::GetHash($e) AS e,
    Yson::GetHash($f) AS f,
    Yson::GetHash($g) AS g,
    Yson::GetHash($h) AS h,
    Yson::GetHash($i) AS i,
    Yson::GetHash($j) AS j,


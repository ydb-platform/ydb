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

SELECT
    Yson::Equals($a1, $a1) AS a1,
    Yson::Equals($a1, $a2) AS a2,
    Yson::Equals($a1, $a3) AS a3,
    Yson::Equals($a1, $b) AS b,
    Yson::Equals($a1, $c) AS c,
    Yson::Equals($a1, $d) AS d,
    Yson::Equals($a1, $e) AS e,
    Yson::Equals($a1, $f) AS f,
    Yson::Equals($a1, $g) AS g,
    Yson::Equals($a1, $h) AS h,
    Yson::Equals($a1, $i) AS i,
    Yson::Equals($h, $i) AS attrs1,
    Yson::Equals($i, $i) AS attrs2;

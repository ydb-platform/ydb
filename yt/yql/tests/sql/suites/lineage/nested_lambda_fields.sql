USE plato;

$v = ($x) -> {
    return ListFilter(["a","b"],($y)->($y = $x));
};

$k = ($x) -> {
    return $x;
};


insert into Output
with truncate
select
    $k(key) as k,
    $v(value) as v
from
    Input

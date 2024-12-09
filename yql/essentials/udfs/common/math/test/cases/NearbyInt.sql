$f = ($mode)->{
    return (
        Math::NearbyInt(Double("NaN"),$mode),
        Math::NearbyInt(1e100,$mode),
        Math::NearbyInt(2.3,$mode),
        Math::NearbyInt(2.5,$mode),
        Math::NearbyInt(2.7,$mode),
        Math::NearbyInt(3.5,$mode),
        Math::NearbyInt(-1e100,$mode),
        Math::NearbyInt(-2.3,$mode),
        Math::NearbyInt(-2.5,$mode),
        Math::NearbyInt(-2.7,$mode),
        Math::NearbyInt(-3.5,$mode)
    )
};

select $f(Math::RoundDownward()), 0 as x
union all
select $f(Math::RoundToNearest()), 1 as x
union all
select $f(Math::RoundTowardZero()), 2 as x
union all
select $f(Math::RoundUpward()), 3 as x
order by x;

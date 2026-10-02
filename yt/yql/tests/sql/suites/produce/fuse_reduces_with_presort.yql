USE plato;

$reduce = ($_, $TableRows) -> {
    return Yql::Condense1(
        $TableRows,
        ($item) -> ($item),
        ($_, $_) -> (false),
        ($item, $_) -> ($item)
    );
};

$stream = 
select * from Input;

--

$stream1 = Reduce $stream
presort value1
on key, subkey
using $reduce(TableRow())
assume order by key, subkey,  value1;

$stream1 = Reduce $stream1
presort value1
on key, subkey
using $reduce(TableRow())
assume order by key, subkey, value1;

--

$stream2 = Reduce $stream
presort value1, value2
on key, subkey
using $reduce(TableRow())
assume order by key, subkey,  value1, value2;

$stream2 = Reduce $stream2
presort value1
on key, subkey
using $reduce(TableRow())
assume order by key, subkey, value1;

--

$stream3 = Reduce $stream
presort value1, value2, value3
on key, subkey
using $reduce(TableRow())
assume order by key, subkey,  value1, value2, value3;

$stream3 = Reduce $stream3
on key, subkey
using $reduce(TableRow())
assume order by key, subkey;

select
    *
from $stream1
ASSUME ORDER BY `key`, `subkey`;

select
    *
from $stream2
ASSUME ORDER BY `key`, `subkey`;

select
    *
from $stream3
ASSUME ORDER BY `key`, `subkey`;

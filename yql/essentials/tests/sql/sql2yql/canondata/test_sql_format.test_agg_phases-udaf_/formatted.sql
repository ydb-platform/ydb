/*
  Double  is an item
  Python3 is a  state
  String  is a  result
  Int64   is a  serialized
*/
$script = @@#py
import json

class State:
    def __init__(self, state):
        self.state = state

def create(item):
    return State(item)

def add(state, item):
    return State(state.state + item)

def merge(state_a, state_b):
    return State(state_a.state + state_b.state)

def get_result(state):
    return str(state.state)

def serialize(state):
    return int(state.state)

def deserialize(serialized):
    return State(float(serialized))
@@;

$create = Python3::create(Callable<(Double) -> Resource<Python3>>, $script);
$add = Python3::add(Callable<(Resource<Python3>, Double) -> Resource<Python3>>, $script);
$merge = Python3::merge(Callable<(Resource<Python3>, Resource<Python3>) -> Resource<Python3>>, $script);
$get_result = Python3::get_result(Callable<(Resource<Python3>) -> String>, $script);
$serialize = Python3::serialize(Callable<(Resource<Python3>) -> Int64>, $script);
$deserialize = Python3::deserialize(Callable<(Int64) -> Resource<Python3>>, $script);
$default = '';

$factory = AggregationFactory(
    'UDAF',
    $create,
    $add,
    $merge,
    $get_result,
    $serialize,
    $deserialize,
    $default
);

$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|key: 1, item: 1.0|>,
            <|key: 1, item: 2.0|>,
            <|key: 1, item: -2.0|>,
            <|key: 2, item: 1.0|>,
            <|key: 2, item: 1.0|>,
            <|key: 3, item: 1.0|>,
            <|key: 3, item: 2.0|>,
        ])
);

$states = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|key: 1, state: $serialize($create(1))|>,
            <|key: 1, state: $serialize($create(2))|>,
            <|key: 1, state: $serialize($create(-2))|>,
            <|key: 2, state: $serialize($create(1))|>,
            <|key: 2, state: $serialize($create(1))|>,
            <|key: 3, state: $serialize($create(1))|>,
            <|key: 3, state: $serialize($create(2))|>,
        ])
);

$p = (
    SELECT
        key,
        AGGREGATE_BY(item, $factory) AS state
    FROM
        $input
    GROUP BY
        key
        WITH combine
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        AGGREGATE_BY(item, $factory) AS result
    FROM
        $input
    GROUP BY
        key
        WITH finalize
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        AGGREGATE_BY(state, $factory) AS state
    FROM
        $states
    GROUP BY
        key
        WITH combinestate
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        AGGREGATE_BY(state, $factory) AS state
    FROM
        $states
    GROUP BY
        key
        WITH mergestate
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        AGGREGATE_BY(state, $factory) AS result
    FROM
        $states
    GROUP BY
        key
        WITH mergefinalize
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

$p = (
    SELECT
        key,
        AGGREGATE_BY(state, $factory) AS result
    FROM (
        SELECT
            key,
            just(state) AS state
        FROM
            $states
    )
    GROUP BY
        key
        WITH mergemanyfinalize
);

$p = (
    PROCESS $p
);

SELECT
    *
FROM
    $p
;

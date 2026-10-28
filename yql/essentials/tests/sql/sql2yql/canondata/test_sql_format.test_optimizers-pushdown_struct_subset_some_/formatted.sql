PRAGMA config.flags('OptimizerFlags', 'PushdownStructSubsetFieldsOverAggregate', 'PropagateSomeTraitsUnusedColumns');
PRAGMA warning('disable', '4510');

SELECT
    a,
    b
FROM (
    SELECT
        some(TableRow())
    FROM
        as_table([<|a: 1, b: 2, c: 3|>])
)
    FLATTEN COLUMNS
;

$identity = ($value) -> ($value);

$makeFactory = ($update) -> (
    ($itemType, $extractor) -> (
        YQL::AggregationTraits(
            $itemType,
            $extractor,
            $update,
            $identity,
            $identity,
            ($left, $_right) -> ($left),
            $identity,
            NULL
        )
    )
);

$factory = $makeFactory(($_row, $state) -> (<|kept: $state.pruned, pruned: $state.pruned|>));

-- Multirow cases keep the selected value equal in all reachable states,
-- since AGGREGATE_BY guarantees neither input order nor merge order.
SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factory) AS result
    FROM
        as_table(ListCreate(Struct<kept: Int32, pruned: Int32>))
);

SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factory) AS result
    FROM
        as_table([<|kept: 1, pruned: 10|>])
);

SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factory) AS result
    FROM
        as_table([
            <|kept: 10, pruned: 10|>,
            <|kept: 10, pruned: 10|>
        ])
);

$factoryExtra = $makeFactory(
    ($_row, $state) -> (
        <|kept: $state.pruned, pruned: $state.pruned, unused: 0|>
    )
);

SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factoryExtra) AS result
    FROM
        as_table([
            <|kept: 10, pruned: 10, unused: 0|>,
            <|kept: 10, pruned: 10, unused: 1|>
        ])
);

$factoryWholeState = $makeFactory(($row, $state) -> (IF($state == $row, $state, $row)));

SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factoryWholeState) AS result
    FROM
        as_table([
            <|kept: 1, pruned: 10|>,
            <|kept: 1, pruned: 20|>
        ])
);

$factoryWithParent = ($itemType, $extractor) -> (
    YQL::AggregationTraits(
        $itemType,
        ($row, $parent) -> (
            <|
                kept: $extractor($row).kept + CAST($parent AS Int32),
                pruned: $extractor($row).pruned,
                unused: 0
            |>
        ),
        ($_row, $state, $parent) -> (
            <|
                kept: $state.pruned + CAST($parent AS Int32),
                pruned: $state.pruned,
                unused: 0
            |>
        ),
        $identity,
        $identity,
        ($left, $_right) -> ($left),
        $identity,
        NULL
    )
);

SELECT
    result.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factoryWithParent) AS result
    FROM
        as_table([<|kept: 1, pruned: 10|>])
);

SELECT
    result.kept,
    other.kept
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $factoryWithParent) AS result,
        AGGREGATE_BY(<|kept: kept + 100, pruned: pruned + 100|>, $factoryWithParent) AS other
    FROM
        as_table([
            <|kept: 10, pruned: 10|>,
            <|kept: 10, pruned: 10|>
        ])
);

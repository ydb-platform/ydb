$t = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|id:1, key:"foo", value: "v1", docIds:[10u, 42u]|>,
            <|id:2, key:"foo", value: "v2", docIds:[94u]|>,
            <|id:3, key:"baz", value: "v3", docIds:[78u, 94u]|>
            ]
        )
);

$search = [42u, 94u];

$filter_lambda = ($search, $doc_ids) -> {
    return DictHasItems(SetIntersection(ToSet($search), ToSet($doc_ids)));
};

$udaf = AggregationFactory("UDAF",
    ($item, $parent) -> (Roaring::FromUint32List(AsList(UNWRAP(CAST($item AS Uint32))), $parent)),
    ($state, $item, $_) -> (Roaring::Add($state, UNWRAP(CAST($item AS Uint32)))),
    ($state1, $state2) -> (Roaring::Or($state1, $state2)),
    ($state) -> (Roaring::Uint32List($state)),
    ($state) -> (Roaring::Serialize($state)),
    ($state) -> (Roaring::Deserialize($state)));

SELECT
    key,
    AGGREGATE_BY(id, $udaf)
FROM
    $t
WHERE
    $filter_lambda($search, docIds)
GROUP BY
    key
ORDER BY
    key

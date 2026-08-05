$e = TypeOf(AsErased(NULL));
$nodeType = Struct<value: String, left: Optional<$e>, right: Optional<$e>>;

$makeNode = ($v, $l, $r) -> {
    RETURN CAST(
        <|
            value: $v,
            left: if($l IS NOT NULL, AsErased($l)),
            right: if($r IS NOT NULL, AsErased($r))
        |> AS $nodeType
    );
};

$leftLink = ($node) -> {
    RETURN Unwrap(PeekErased(Unwrap($node.left), $nodeType));
};

$rightLink = ($node) -> {
    RETURN Unwrap(PeekErased(Unwrap($node.right), $nodeType));
};

$node1 = $makeNode('A', NULL, NULL);
$node2 = $makeNode('B', $node1, NULL);
$node3 = $makeNode('C', NULL, NULL);
$node4 = $makeNode('D', $node2, $node3);

$callableType = Callable<($e, $nodeType) -> String>;

$printImpl = ($impl_erased, $node) -> {
    $impl = Unwrap(PeekErased($impl_erased, $callableType));
    RETURN '{'
        || $node.value
        || if($node.left IS NOT NULL, ',left:' || $impl($impl_erased, $leftLink($node)), '')
        || if($node.right IS NOT NULL, ',right:' || $impl($impl_erased, $rightLink($node)), '')
        || '}';
};

$print = ($node) -> ($printImpl(AsErased(Callable($callableType, $printImpl)), $node));

SELECT
    $print($node4)
;

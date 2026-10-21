$erasedType = TypeOf(AsErased(NULL));
$nodeType = Struct<value: String, left: Optional<$erasedType>, right: Optional<$erasedType>>;

$makeNode = ($value, $left, $right) -> {
    RETURN CAST(
        <|
            value: $value,
            left: if($left IS NOT NULL, AsErased($left)),
            right: if($right IS NOT NULL, AsErased($right))
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

$thunkType = Callable<() -> $erasedType>;
$contType = Callable<($erasedType) -> $erasedType>;
$trampolineCallableType = Callable<($erasedType, $contType, $nodeType) -> $erasedType>;

$printImplTrampoline = ($impl_erased, $cont, $node) -> {
    $impl = Unwrap(PeekErased($impl_erased, $trampolineCallableType));
    $str = '{' || $node.value;
    RETURN if(
        $node.left IS NULL AND $node.right IS NULL,
        $cont(AsErased($str || '}')),
        AsErased(
            Callable(
                $thunkType, () -> {
                    $leftCont = ($leftValue) -> {
                        $rightCont = ($rightValue) -> {
                            $leftStr = Unwrap(PeekErased($leftValue, String));
                            $rightStr = Unwrap(PeekErased($rightValue, String));
                            $leftStr = if(len($leftStr) > 0, ',left:' || $leftStr, '');
                            $rightStr = if(len($rightStr) > 0, ',right:' || $rightStr, '');
                            RETURN $cont(AsErased($str || $leftStr || $rightStr || '}'));
                        };
                        RETURN if(
                            $node.right IS NOT NULL,
                            $impl($impl_erased, Callable($contType, $rightCont), $rightLink($node)),
                            $rightCont(AsErased(''))
                        );
                    };
                    RETURN if(
                        $node.left IS NOT NULL,
                        $impl($impl_erased, Callable($contType, $leftCont), $leftLink($node)),
                        $leftCont(AsErased(''))
                    );
                }
            )
        )
    );
};

$printTrampoline = ($node) -> {
    $impl_erased = AsErased(Callable($trampolineCallableType, $printImplTrampoline));
    RETURN $printImplTrampoline($impl_erased, Callable($contType, ($x) -> ($x)), $node);
};

$runTrampoline = ($res, $limit, $type) -> {
    $values = ListFoldMap(
        ListFromRange(0, $limit), $res, ($_, $res) -> {
            $thunk = PeekErased($res, $thunkType);
            $ret = if(
                $thunk IS NOT NULL,
                ((TRUE, $res), Unwrap($thunk)()),
                ((FALSE, $res), $res)
            );
            RETURN ($ret.0, $ret.1);
        }
    );
    $ret = ListHead(ListSkipWhile($values, ($x) -> ($x.0))).1;
    RETURN if($ret IS NOT NULL, PeekErased(Unwrap($ret), $type));
};

SELECT
    $runTrampoline($printTrampoline($node4), 10, String),
    $runTrampoline($printTrampoline($node4), 2, String)
; -- null, not enough iterations

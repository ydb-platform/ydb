$t = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|andMask: Roaring::Serialize(Roaring::FromUint32List(AsList(11u, 567u, 42u))), orMask: Roaring::Serialize(Roaring::FromUint32List(AsList(11u)))|>,
            <|andMask: Roaring::Serialize(Roaring::FromUint32List(AsList(11u, 567u))), orMask: Roaring::Serialize(Roaring::FromUint32List(AsList(567u)))|>,
            <|andMask: Roaring::Serialize(Roaring::FromUint32List(AsList(11u))), orMask: Roaring::Serialize(Roaring::FromUint32List(AsList(42u)))|>,
	    <|andMask: Roaring::Serialize(Roaring::FromUint32List(AsList(5u))), orMask: Roaring::Serialize(Roaring::FromUint32List(ListCreate(Uint32)))|>
            ]
        )
);

$orRegular = Roaring::FromUint32List(AsList(1u, 11u));
$orModifiable = Roaring::FromUint32List(AsList(2u, 11u));
$andRegular = Roaring::FromUint32List(AsList(3u, 11u));
$andModifiable = Roaring::FromUint32List(AsList(4u, 11u));

SELECT
    Roaring::Uint32List(Roaring::OrWithBinary($orRegular, orMask, false)) as RegularOr,
    Roaring::Uint32List(Roaring::OrWithBinary($orModifiable, orMask, true)) as MutableOr,
    Roaring::Uint32List(Roaring::AndWithBinary($andRegular, andMask, false)) as ReguarAnd,
    Roaring::Uint32List(Roaring::AndWithBinary($andModifiable, andMask, true)) as MutableAnd
FROM
    $t;

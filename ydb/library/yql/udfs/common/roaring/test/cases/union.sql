SELECT Roaring::Uint32List(Roaring::Or(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS OrList FROM Input;
SELECT Roaring::Uint32List(Roaring::OrWithBinary(Roaring::Deserialize(right), left)) AS OrWithBinaryList FROM Input;

SELECT Roaring::Uint32List(Roaring::Or(Roaring::Deserialize(left), Roaring::Deserialize(right), true)) AS OrListInplace FROM Input;
SELECT Roaring::Uint32List(Roaring::OrWithBinary(Roaring::Deserialize(right), left, true)) AS OrWithBinaryListInplace FROM Input;

SELECT Roaring::IsEmpty(Roaring::Or(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS UnionIsNotEmpty FROM Input;


SELECT Roaring::Uint32List(Roaring::Add(Roaring::FromUint32List(AsList(10)), 20)) AS UnionViaAdd;


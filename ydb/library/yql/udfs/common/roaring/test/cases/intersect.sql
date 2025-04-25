SELECT Roaring::Uint32List(Roaring::And(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS AndList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), left)) AS AndWithBinaryList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), NULL)) AS AndWithBinaryListEmpty FROM Input;

SELECT Roaring::Uint32List(Roaring::AndNot(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS AndNotList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndNotWithBinary(Roaring::Deserialize(right), left)) AS AndNotWithBinaryList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndNotWithBinary(Roaring::Deserialize(right), NULL)) AS AndNotWithBinaryListEmpty FROM Input;

SELECT Roaring::Uint32List(Roaring::And(Roaring::Deserialize(left), Roaring::Deserialize(right), true)) AS AndListInplace FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), left, true)) AS AndWithBinaryListInplace FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), NULL, true)) AS AndWithBinaryListEmptyInplace FROM Input;

SELECT Roaring::Uint32List(Roaring::AndNot(Roaring::Deserialize(left), Roaring::Deserialize(right), true)) AS AndNotListInplace FROM Input;
SELECT Roaring::Uint32List(Roaring::AndNotWithBinary(Roaring::Deserialize(right), left, true)) AS AndNotWithBinaryListInplace FROM Input;
SELECT Roaring::Uint32List(Roaring::AndNotWithBinary(Roaring::Deserialize(right), NULL, true)) AS AndNotWithBinaryListEmptyInplace FROM Input;

SELECT Roaring::Uint32List(Roaring::NaiveBulkAnd(AsList(Roaring::Deserialize(right), Roaring::Deserialize(left)))) AS NaiveBulkAnd FROM Input;
SELECT Roaring::Uint32List(Roaring::NaiveBulkAndWithBinary(AsList(right, left))) AS NaiveBulkAndWithBinary FROM Input;

SELECT Roaring::Intersect(Roaring::Deserialize(right), Roaring::Deserialize(left)) AS Intersect FROM Input;
SELECT Roaring::Intersect(NULL,  Roaring::Deserialize(left)) AS IntersectNull0 FROM Input;
SELECT Roaring::Intersect(Roaring::Deserialize(right), NULL) AS IntersectNull1 FROM Input;
SELECT Roaring::IntersectWithBinary(Roaring::Deserialize(right), left) AS IntersectWithBinary FROM Input;
SELECT Roaring::IntersectWithBinary(Roaring::Deserialize(right), NULL) AS IntersectWithBinaryNull FROM Input;
SELECT Roaring::Intersect(Roaring::Deserialize(right), Roaring::FromUint32List(AsList(100500))) AS IntersectFalse FROM Input;

SELECT Roaring::IsEmpty(Roaring::And(Roaring::Deserialize(right), Roaring::FromUint32List(AsList(100500)))) AS IntersectionIsEmpty FROM Input;
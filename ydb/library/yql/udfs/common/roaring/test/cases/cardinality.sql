SELECT Roaring::Cardinality(Roaring::UnionWithBinary(Roaring::Deserialize(left), right)) AS UnionCardinality FROM Input;
SELECT Roaring::Uint32List(Roaring::IntersectWithBinary(Roaring::Deserialize(right), left)) AS IntersectCardinality FROM Input;

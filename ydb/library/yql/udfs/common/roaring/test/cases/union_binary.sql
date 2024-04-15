SELECT Roaring::Cardinality(Roaring::UnionWithBinary(Roaring::Deserialize(left), right)) AS UnionWithBinaryList FROM Input;


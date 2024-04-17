SELECT Roaring::Cardinality(Roaring::OrWithBinary(Roaring::Deserialize(left), right)) AS OrCardinality FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), left)) AS AndCardinality FROM Input;

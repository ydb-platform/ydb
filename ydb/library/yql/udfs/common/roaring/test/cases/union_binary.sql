SELECT Roaring::Cardinality(Roaring::OrWithBinary(Roaring::Deserialize(left), right)) AS OrWithBinaryList FROM Input;


SELECT Roaring::Uint32List(Roaring::Or(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS OrList FROM Input;
SELECT Roaring::Uint32List(Roaring::OrWithBinary(Roaring::Deserialize(right), left)) AS OrWithBinaryList FROM Input;


SELECT Roaring::Uint32List(Roaring::And(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS AndList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), left)) AS AndWithBinaryList FROM Input;
SELECT Roaring::Uint32List(Roaring::AndWithBinary(Roaring::Deserialize(right), NULL)) AS AndWithBinaryListEmpty FROM Input;

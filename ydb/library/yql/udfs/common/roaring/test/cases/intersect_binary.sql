SELECT Roaring::Uint32List(Roaring::IntersectWithBinary(Roaring::Deserialize(left), right)) AS IntersectWithBinaryList FROM Input;

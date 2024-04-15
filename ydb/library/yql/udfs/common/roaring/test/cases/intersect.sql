SELECT Roaring::Uint32List(Roaring::Intersect(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS IntersectList FROM Input;

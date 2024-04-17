SELECT Roaring::Uint32List(Roaring::And(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS AndList FROM Input;

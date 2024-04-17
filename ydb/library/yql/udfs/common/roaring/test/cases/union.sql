SELECT Roaring::Cardinality(Roaring::Or(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS OrList FROM Input;


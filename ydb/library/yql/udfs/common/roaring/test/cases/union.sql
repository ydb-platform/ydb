SELECT Roaring::Cardinality(Roaring::Union(Roaring::Deserialize(left), Roaring::Deserialize(right))) AS UnionList FROM Input;


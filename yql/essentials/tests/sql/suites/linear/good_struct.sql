$x = MutDictHasItems(MutDictCreate(Int32,String,0));
select FromMutDict(ExpandStruct(WithSideEffects(Opaque(<|z:Void()|>)),$x.0 as a,$x.1 as b).a)

/* postgres can not */
/* syntax version 1 */
select 
    StructMembers(<|a:1|>),
    StructMembers(Just(<|a:1|>)),
    StructMembers(null),
    
    GatherMembers(<||>),
    GatherMembers(<|a:1,b:2|>),
    GatherMembers(Just(<|a:1,b:2|>)),
    GatherMembers(null),

    RenameMembers(<|a:1,c:2|>,[("a","b")]),
    RenameMembers(<|a:1,c:2|>,[("a","b"),("a","d")]),
    ForceRenameMembers(<|a:1,c:2|>,[("a","b")]),
    ForceRenameMembers(<|a:1,c:2|>,[("d","d")]),
    RenameMembers(Just(<|a:1,c:2|>),[("a","b")]),
    RenameMembers(null,[("a","b")]),
    
    SpreadMembers([("a",1)],["a","b"]),
    SpreadMembers([("a",1),("b",2)],["a","b"]),
    SpreadMembers([("a",Just(1))],["a","b"]),
    SpreadMembers([("a",1),("a",2)],["a","b"]),
    SpreadMembers([],["a","b"]),
    
    SpreadMembers(Just([("a",1)]),["a","b"]),
    SpreadMembers(null,["a","b"]),
    ForceSpreadMembers([("a",1)],["a","b"]),
    ForceSpreadMembers([("c",1)],["a","b"]);

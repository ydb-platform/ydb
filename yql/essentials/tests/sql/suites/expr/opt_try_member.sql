/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
select 
TryMember(Just(<|x:1|>),"x",0),
TryMember(Just(<|x:1|>),"y",0),
TryMember(Just(<|x:Just(1)|>),"x",Just(0)),
TryMember(Just(<|x:Just(1)|>),"y",Just(0)),
TryMember(Just(<|x:Nothing(Int32?)|>),"x",Just(0)),
TryMember(Just(<|x:Nothing(Int32?)|>),"y",Just(0)),

TryMember(Just(<|x:1|>),"x",null),
TryMember(Just(<|x:1|>),"y",null),
TryMember(Just(<|x:Just(1)|>),"x",null),
TryMember(Just(<|x:Just(1)|>),"y",null),
TryMember(Just(<|x:Nothing(Int32?)|>),"x",null),
TryMember(Just(<|x:Nothing(Int32?)|>),"y",null),

TryMember(Nothing(Struct<x:Int32>?),"x",0),
TryMember(Nothing(Struct<x:Int32>?),"x",null),

TryMember(Nothing(Struct<x:Int32?>?),"x",Just(0)),
TryMember(Nothing(Struct<x:Int32?>?),"x",null);

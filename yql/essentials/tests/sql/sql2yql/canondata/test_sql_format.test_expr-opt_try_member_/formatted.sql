/* postgres can not */
/* kikimr can not */
/* syntax version 1 */
SELECT
    TryMember(Just(<|x: 1|>), 'x', 0),
    TryMember(Just(<|x: 1|>), 'y', 0),
    TryMember(Just(<|x: Just(1)|>), 'x', Just(0)),
    TryMember(Just(<|x: Just(1)|>), 'y', Just(0)),
    TryMember(Just(<|x: Nothing(Int32?)|>), 'x', Just(0)),
    TryMember(Just(<|x: Nothing(Int32?)|>), 'y', Just(0)),
    TryMember(Just(<|x: 1|>), 'x', NULL),
    TryMember(Just(<|x: 1|>), 'y', NULL),
    TryMember(Just(<|x: Just(1)|>), 'x', NULL),
    TryMember(Just(<|x: Just(1)|>), 'y', NULL),
    TryMember(Just(<|x: Nothing(Int32?)|>), 'x', NULL),
    TryMember(Just(<|x: Nothing(Int32?)|>), 'y', NULL),
    TryMember(Nothing(Struct<x: Int32>?), 'x', 0),
    TryMember(Nothing(Struct<x: Int32>?), 'x', NULL),
    TryMember(Nothing(Struct<x: Int32?>?), 'x', Just(0)),
    TryMember(Nothing(Struct<x: Int32?>?), 'x', NULL)
;

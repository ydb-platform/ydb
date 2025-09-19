/* syntax version 1 */
/* postgres can not */
select
    Yql::AggrEquals([],[]),
    Yql::AggrNotEquals([],[]),
    Yql::AggrLess([],[]),
    Yql::AggrLessOrEqual([],[]),
    Yql::AggrGreater([],[]),
    Yql::AggrGreaterOrEqual([],[]),
    
    Yql::AggrEquals({},{}),
    Yql::AggrNotEquals({},{}),
    
    [] = [],
    [] = ListCreate(Int32),
    ListCreate(Int32) = [],
    [] = [1],
    [1] = [],
    
    [] != [],
    [] != ListCreate(Int32),
    ListCreate(Int32) != [],
    [] != [1],
    [1] != [],

    [] < [],
    [] < ListCreate(Int32),
    ListCreate(Int32) < [],
    [] < [1],
    [1] < [],

    [] <= [],
    [] <= ListCreate(Int32),
    ListCreate(Int32) <= [],
    [] <= [1],
    [1] <= [],

    [] > [],
    [] > ListCreate(Int32),
    ListCreate(Int32) > [],
    [] > [1],
    [1] > [],

    [] >= [],
    [] >= ListCreate(Int32),
    ListCreate(Int32) >= [],
    [] >= [1],
    [1] >= [],

    {} = {},
    {} = SetCreate(Int32),
    SetCreate(Int32) = {},
    {} = {1},
    {1} = {},
    
    {} != {},
    {} != SetCreate(Int32),
    SetCreate(Int32) != {},
    {} != {1},
    {1} != {},

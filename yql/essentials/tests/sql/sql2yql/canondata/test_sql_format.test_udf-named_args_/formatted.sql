/* postgres can not */
SELECT
    SimpleUdf::NamedArgs(0) AS named_args0,
    SimpleUdf::NamedArgs(1, 101 AS D, 13 AS C) AS named_args1,
    SimpleUdf::NamedArgs(1, 21 AS C, 113 AS D) AS named_args2,
    SimpleUdf::NamedArgs(1, 42 AS C) AS named_args3,
    SimpleUdf::NamedArgs(1, 128 AS D) AS named_args4,
    SimpleUdf::NamedArgs(100, 500, 404 AS D, 44 AS C) AS named_args5,
    SimpleUdf::NamedArgs(100, 500, 55 AS C, 505 AS D) AS named_args6,
    SimpleUdf::NamedArgs(100, 500, 25 AS C) AS named_args7,
    SimpleUdf::NamedArgs(100, 500, 606 AS D) AS named_args8,
    SimpleUdf::NamedArgs(100, 500, 64, 512 AS D) AS named_args9
;

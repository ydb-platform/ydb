/* syntax version 1 */

select
Yson::Contains(Yson::Parse('{a=1}'),'a'),
Yson::Contains(Yson::Parse('{a=1}'),'b'),
Yson::Contains(Yson::Parse('[]'),'0'),
Yson::Contains(Yson::Parse('[1;2]'),'0'),
Yson::Contains(Yson::Parse('[1;2]'),'2'),
Yson::Contains(Yson::Parse('[1;2]'),'-2'),
Yson::Contains(Yson::Parse('[1;2]'),'-3'),
Yson::Contains(Yson::Parse('2'),'2', Yson::Options(false AS Strict));

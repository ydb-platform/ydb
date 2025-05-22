/* syntax version 1 */

select
Yson::SerializeText(Yson::WithAttributes(Yson::Parse('1'), Yson::Parse('{a=2}'))),
Yson::SerializeText(Yson::WithAttributes(Yson::Parse('1'), Yson::Parse('{}'))),
Yson::SerializeText(Yson::WithAttributes(Yson::Parse('1'), Yson::Parse('#'))),
Yson::SerializeText(Yson::WithAttributes(Yson::Parse('<c=2>1'), Yson::Parse('{b=3}')));

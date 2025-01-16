/* syntax version 1 */
select
Yson::SerializeText(Yson::Parse(Yson('<a=1>[#;{a=1};{b=2u;c=[]};<q=foo>3.0;{};foo;"very loooooooooooooooooong string"]')));

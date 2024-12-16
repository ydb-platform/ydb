/* syntax version 1 */
select 
    Yson::Parse(Yson::Parse("[]"y)),
    Yson::ParseJson(Yson::Parse("[]"y));

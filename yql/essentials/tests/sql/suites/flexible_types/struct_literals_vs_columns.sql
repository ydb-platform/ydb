/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;

select <|'type_' || FormatType(TypeOf(text)):text|> as result from (select 1 as text);
select <|text:''|>;

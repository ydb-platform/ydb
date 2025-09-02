/* syntax version 1 */
SELECT
    Unicode::ToCodePointList(value) AS code_point_list,
    Unicode::FromCodePointList(Unicode::ToCodePointList(value)) AS from_code_point_list,
    Unicode::FromCodePointList(YQL::LazyList(Unicode::ToCodePointList(value))) AS from_lazy_code_point_list,
FROM Input

$pattern_from_like = Udf(Re2::PatternFromLikeSpark);
SELECT $pattern_from_like(pattern, escape) AS pattern,
       Re2::Match($pattern_from_like(pattern, escape), Re2::Options())(value) AS matched
FROM AS_TABLE(AsList(
    AsStruct('' AS value, '' AS pattern, '\\' AS escape),
    AsStruct('' AS value, '%' AS pattern, '\\' AS escape),
    AsStruct('' AS value, '_' AS pattern, '\\' AS escape),
    AsStruct('😀' AS value, '_' AS pattern, '\\' AS escape),
    AsStruct('a\nb' AS value, 'a_b' AS pattern, '\\' AS escape),
    AsStruct('a.b' AS value, 'a.b' AS pattern, '\\' AS escape),
    AsStruct('a%b' AS value, 'a\\%b' AS pattern, '\\' AS escape),
    AsStruct('a_b' AS value, 'a\\_b' AS pattern, '\\' AS escape),
    AsStruct('a\\b' AS value, 'a\\\\b' AS pattern, '\\' AS escape),
    AsStruct('a%b' AS value, 'a!%b' AS pattern, '!' AS escape),
    AsStruct('a%b' AS value, 'aж%b' AS pattern, 'ж' AS escape),
    AsStruct('ж' AS value, 'жж' AS pattern, 'ж' AS escape),
    AsStruct('%' AS value, '%%' AS pattern, '%' AS escape),
    AsStruct('_' AS value, '__' AS pattern, '_' AS escape),
    AsStruct('[' AS value, '[[' AS pattern, '[' AS escape)
));

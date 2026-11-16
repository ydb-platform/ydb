$pattern_from_like = Udf(Re2::PatternFromLikeSpark);
SELECT $pattern_from_like('a\\b', '\\');

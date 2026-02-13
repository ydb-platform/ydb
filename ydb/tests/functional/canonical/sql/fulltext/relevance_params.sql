DECLARE $query as String;
SELECT Key, Text, FulltextScore(Text, $query) as relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY relevance DESC;

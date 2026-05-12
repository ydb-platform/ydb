DECLARE $query as String;
SELECT Key, Text, FulltextScore(Text, $query) as relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, $query) > 0
ORDER BY relevance DESC;

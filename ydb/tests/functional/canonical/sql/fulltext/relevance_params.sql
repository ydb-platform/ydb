DECLARE $query as String;
SELECT Key, Text, FullText::Relevance(Text, $query) as relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY relevance DESC;

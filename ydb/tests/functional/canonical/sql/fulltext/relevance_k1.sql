SELECT Key, Text, FulltextScore(Text, "cats dogs", 1.5 as K1) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, "cats dogs", 1.5 as K1) > 0
ORDER BY Relevance DESC;

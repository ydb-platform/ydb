SELECT Key, Text, FulltextScore(Text, "cats dogs", "or" as DefaultOperator) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, "cats dogs", "or" as DefaultOperator) > 0
ORDER BY Relevance DESC;

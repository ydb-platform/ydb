SELECT Key, Text, FulltextScore(Text, "cats dogs", "and" as DefaultOperator) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

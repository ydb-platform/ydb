SELECT Key, Text, FulltextScore(Text, "cats dogs", "and" as DefaultOperator, 0.75 as B) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, "cats dogs", "and" as DefaultOperator, 0.75 as B) > 0
ORDER BY Relevance DESC;

SELECT Key, Text, FulltextScore(Text, "cats dogs", "or" as DefaultOperator, "1" as MinimumShouldMatch) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, "cats dogs", "or" as DefaultOperator, "1" as MinimumShouldMatch) > 0
ORDER BY Relevance DESC;

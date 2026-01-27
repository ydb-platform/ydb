SELECT Key, Text, FulltextScore(Text, "cats dogs", "or" as Mode, "1" as MinimumShouldMatch) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

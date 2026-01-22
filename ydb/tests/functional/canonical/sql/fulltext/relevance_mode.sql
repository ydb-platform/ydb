SELECT Key, Text, FulltextScore(Text, "cats dogs", "or" as Mode) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

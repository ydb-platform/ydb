SELECT Key, Text, FulltextScore(Text, "cats") as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

SELECT Key, Text, Data, FulltextScore(Text, "cats") as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

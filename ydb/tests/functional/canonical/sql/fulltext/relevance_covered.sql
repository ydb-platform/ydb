SELECT Key, Text, Data, FulltextScore(Text, "cats") as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
WHERE FulltextScore(Text, "cats") > 0
ORDER BY Relevance DESC;

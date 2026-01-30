SELECT Key, Text, FulltextScore(Text, "cats dogs", 1.2 as K1, 0.75 as B) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

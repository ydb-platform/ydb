SELECT Key, Text, FullText::Relevance(Text, "cats") as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

SELECT Key, Text, FullText::Relevance(Text, "cats dogs", "and" as Mode, 0.75 as B) as Relevance
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY Relevance DESC;

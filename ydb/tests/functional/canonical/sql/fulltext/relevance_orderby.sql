SELECT Key, Text
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY FullText::Relevance(Text, "cats") DESC
LIMIT 10;

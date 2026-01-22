SELECT Key, Text
FROM FullTextTable VIEW fulltext_relevance_idx
ORDER BY FulltextScore(Text, "cats") DESC
LIMIT 10;

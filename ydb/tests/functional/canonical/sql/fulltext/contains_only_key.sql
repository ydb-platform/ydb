SELECT Key
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextContains(Text, "dogs")
ORDER BY Key;

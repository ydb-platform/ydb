SELECT Key, Text, Data
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextContains(Text, "cats")
ORDER BY Key;

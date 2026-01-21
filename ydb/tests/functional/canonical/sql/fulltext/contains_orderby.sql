SELECT Key, Text
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextContains(Text, "cats")
ORDER BY Key;

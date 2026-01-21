SELECT Key, Text, Data
FROM FullTextTable VIEW fulltext_idx
WHERE FullText::Contains(Text, "cats")
ORDER BY Key;

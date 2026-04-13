SELECT Key, Text, Data
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextMatch(Text, "cats")
ORDER BY Key;

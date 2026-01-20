SELECT Key
FROM FullTextTable VIEW fulltext_idx
WHERE FullText::Contains(Text, "dogs")
ORDER BY Key;

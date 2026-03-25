SELECT Key
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextMatch(Text, "dogs")
ORDER BY Key;

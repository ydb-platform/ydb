SELECT Key, Text
FROM FullTextTable VIEW fulltext_idx
WHERE FulltextMatch(Text, "cats");

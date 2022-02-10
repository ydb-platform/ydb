SELECT * FROM Input1
WHERE
    Group == 1 AND Name == "Name1" OR
    Group == 4 AND Name == "Name4" AND Amount != 105 OR
    Group == 4 AND Name == "Name4" AND Amount != 106;

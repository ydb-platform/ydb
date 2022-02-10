SELECT * FROM Input1
WHERE (Group == 1 AND Name == "Name1" OR Group == 4 AND Name == "Name4" OR Group == 6 AND Name == "Name2")
    AND Amount != 106 AND Comment == "Test1";

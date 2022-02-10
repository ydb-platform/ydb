SELECT * FROM Input1
WHERE
    Amount != 104 AND
    (Group >= 3 AND Group < 5 AND Name != "Name1" OR
     Group = 6 AND Name = "Name1" OR
     Group = 10 AND Name = "Name5");

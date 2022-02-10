SELECT * FROM Input1
WHERE
    Group > 1 AND Group <= 3 AND Amount != 103 OR
    Group >= 4 AND Group < 10 AND Amount % 2 = 1;

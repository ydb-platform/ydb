SELECT * FROM Input1
WHERE
    Group = 10 AND Name >= "Name1" AND Name < "Name3" OR
    Group = 10 AND Name >= "Name3" AND Name <= "Name5" OR
    Group = 2;

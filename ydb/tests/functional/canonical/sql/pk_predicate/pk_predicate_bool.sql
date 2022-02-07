SELECT Value FROM Input4
WHERE Key1 = true AND Key2 < true;

SELECT Value FROM Input4
WHERE Key1 = false AND Key2 > false;

SELECT Value FROM Input4
WHERE Key1 = false AND Key2 = true OR Key1 = true AND Key2 = false;

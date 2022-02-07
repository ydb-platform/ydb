--!syntax_v1

UPSERT INTO Input1
SELECT key AS Group, subkey AS Amount, value AS Name
FROM Input;


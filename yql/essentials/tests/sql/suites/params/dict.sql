/* syntax version 1 */
declare $x as Dict<String, Int64?>;
declare $x2 as Dict<Utf8, Int64?>;
select $x["a1"], $x["a2"], $x["a3"], $x2["a1"], $x2["a2"], $x2["a3"]

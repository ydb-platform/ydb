/* syntax version 1 */
-- Force filter evaluation, then Snapshot (do not pair Snapshot in the same SELECT row list).
$ctx = Ctx::New();
$vals = AsList(1, 2, 3);
$afterA = ListMap($vals, ($x) -> { RETURN Filters::ApplyA($ctx, $x) });
$afterB = ListMap($afterA, ($x) -> { RETURN Filters::ApplyB($ctx, $x) });
SELECT $afterB AS rows, Ctx::Snapshot($ctx) AS stats;

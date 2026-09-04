/* syntax version 1 */
-- Shared ctx: CountRow always bumps rows_seen; CountPositive bumps positives when x > 0.
-- Evaluate filters first, then Snapshot (do not mix Snapshot into the same SELECT row list).
$ctx = Ctx::New();
$vals = AsList(-1, 2, 3);
$afterRows = ListMap($vals, ($x) -> { RETURN Ctx::CountRow($ctx, $x) });
$afterPos = ListMap($afterRows, ($x) -> { RETURN Ctx::CountPositive($ctx, $x) });
SELECT $afterPos AS rows, Ctx::Snapshot($ctx) AS stats;
-- expected: rows = [-1, 2, 3], stats = "rows_seen=3;positives=2"

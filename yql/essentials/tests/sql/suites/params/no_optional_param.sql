/* syntax version 1 */
declare $x as Uint32?;
select 2 * coalesce($x, 33);

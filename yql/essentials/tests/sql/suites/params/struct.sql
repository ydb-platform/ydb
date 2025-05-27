/* syntax version 1 */
declare $x as Struct<a:Int64, b:String?>;
select cast($x.a as String) || coalesce($x.b, "zzz");

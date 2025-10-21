/* syntax version 1 */

-- underlying type is tuple
declare $x1 as Variant<String, Int64>;
declare $x2 as Variant<String, Int64>;

-- underlying type is struct
declare $x3 as Variant<a:String, b:Int64>;
declare $x4 as Variant<a:String, b:Int64>;
declare $x5 as Variant<a:String, b:Int64>;

select $x1.0 || cast($x2.1 as String) || $x3.a || cast($x4.b as String) || $x5.a;

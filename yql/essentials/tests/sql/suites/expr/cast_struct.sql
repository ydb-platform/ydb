/* postgres can not */
/* syntax version 1 */
$struct = <|du:3.14, fl:-1.f, i:0, s:"str"u, u:123u|>;
select
    CAST($struct AS Struct<>) as empty,
    CAST($struct AS Struct<du:Uint32?, fl:Uint32?, s:Uint16?, u:Int32?>) as partial,
    CAST($struct AS Struct<x:Uint8?, y:Uint16?, z:Int8?>) as others,
    CAST($struct AS Struct<du:Uint32, fl:Uint32, s:Uint16, u:Int32>) as fail;

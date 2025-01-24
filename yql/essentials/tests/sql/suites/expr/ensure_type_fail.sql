/* syntax version 1 */
/* postgres can not */
/* syntax version 1 */
/* custom error:Mismatch types: Int32 != String (message)*/
select EnsureType(42, String, "message");

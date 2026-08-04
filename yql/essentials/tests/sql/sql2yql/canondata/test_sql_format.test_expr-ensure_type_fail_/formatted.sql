/* custom error:Mismatch types: Int32 != String (message)*/
SELECT
    EnsureType(42, String, 'message')
;

CREATE FUNCTION doubledollarinbody(var1 text) RETURNS text
/* see issue277 */
LANGUAGE plpgsql
AS $_$
DECLARE
  str text;
  BEGIN
    str = $$'foo'$$||var1;
    execute 'select '||str into str;
    return str;
  END
$_$;

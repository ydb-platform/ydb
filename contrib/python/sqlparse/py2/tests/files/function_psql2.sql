CREATE OR REPLACE FUNCTION update_something() RETURNS void AS
$body$
BEGIN
    raise notice 'foo';
END;
$body$
LANGUAGE 'plpgsql' VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;
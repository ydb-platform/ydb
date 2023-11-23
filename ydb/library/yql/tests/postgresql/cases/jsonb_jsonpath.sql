select jsonb '{"a": 12}' @? '$';
select jsonb '{"a": 12}' @? '1';
select jsonb '{"a": 12}' @? '$.a.b';
select jsonb '{"a": 12}' @? '$.b';
select jsonb '{"a": 12}' @? '$.a + 2';

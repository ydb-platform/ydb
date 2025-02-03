/* syntax version 1 */
/* postgres can not */

define subquery $foo() as
  select <|a: 1, b: 2|> as s;
end define;

select s.a as a, s.b as b from $foo();

/* postgres can not */
/* by correct way ;-)
E   assert 'a;;a' == 'a;Void;b'
E     - a;;a
E     + a;Void;b
*/
SELECT 'a', x, 'b' FROM (SELECT Null as x) as sq WHERE x IS NULL;

/* postgres can not */
/* syntax version 1 */
use plato;

insert into @d
select * from as_table(AsList(<|key:Date('2021-01-03')|>))
assume order by key
;

insert into @dt
select * from as_table(AsList(<|key:Datetime('2021-01-03T21:15:30Z')|>))
assume order by key
;

insert into @ts
select * from as_table(AsList(<|key:Timestamp('2021-01-03T21:15:30.673521Z')|>))
assume order by key
;


COMMIT;

SELECT * FROM @d WHERE key > Date('2021-01-02');
SELECT * FROM @d WHERE key >= Date('2021-01-03');
SELECT * FROM @d WHERE key < Date('2021-01-04');
SELECT * FROM @d WHERE key <= Date('2021-01-03');

SELECT * FROM @d WHERE key > Datetime('2021-01-02T23:00:00Z');
SELECT * FROM @d WHERE key >= Datetime('2021-01-02T23:00:00Z');
SELECT * FROM @d WHERE key < Datetime('2021-01-03T20:00:00Z');
SELECT * FROM @d WHERE key <= Datetime('2021-01-03T20:00:00Z');

SELECT * FROM @d WHERE key > Timestamp('2021-01-02T23:32:01.673521Z');
SELECT * FROM @d WHERE key >= Timestamp('2021-01-02T23:32:01.673521Z');
SELECT * FROM @d WHERE key < Timestamp('2021-01-03T00:00:00.673521Z');
SELECT * FROM @d WHERE key <= Timestamp('2021-01-03T00:00:00.673521Z');

-------------------------------------------

SELECT * FROM @dt WHERE key > Date('2021-01-03');
SELECT * FROM @dt WHERE key >= Date('2021-01-03');
SELECT * FROM @dt WHERE key < Date('2021-01-04');
SELECT * FROM @dt WHERE key <= Date('2021-01-04');

SELECT * FROM @dt WHERE key > Datetime('2021-01-03T21:15:29Z');
SELECT * FROM @dt WHERE key >= Datetime('2021-01-03T21:15:30Z');
SELECT * FROM @dt WHERE key < Datetime('2021-01-03T21:15:31Z');
SELECT * FROM @dt WHERE key <= Datetime('2021-01-03T21:15:30Z');

SELECT * FROM @dt WHERE key > Timestamp('2021-01-03T21:15:29.673521Z');
SELECT * FROM @dt WHERE key >= Timestamp('2021-01-03T21:15:29.673521Z');
SELECT * FROM @dt WHERE key < Timestamp('2021-01-03T21:15:30.673521Z');
SELECT * FROM @dt WHERE key <= Timestamp('2021-01-03T21:15:30.673521Z');

-------------------------------------------

SELECT * FROM @ts WHERE key > Date('2021-01-03');
SELECT * FROM @ts WHERE key >= Date('2021-01-03');
SELECT * FROM @ts WHERE key < Date('2021-01-04');
SELECT * FROM @ts WHERE key <= Date('2021-01-04');

SELECT * FROM @ts WHERE key > Datetime('2021-01-03T20:00:00Z');
SELECT * FROM @ts WHERE key >= Datetime('2021-01-03T20:00:00Z');
SELECT * FROM @ts WHERE key < Datetime('2021-01-03T22:00:00Z');
SELECT * FROM @ts WHERE key <= Datetime('2021-01-03T22:00:00Z');

SELECT * FROM @ts WHERE key > Timestamp('2021-01-03T21:15:30.573521Z');
SELECT * FROM @ts WHERE key >= Timestamp('2021-01-03T21:15:30.673521Z');
SELECT * FROM @ts WHERE key < Timestamp('2021-01-03T21:15:30.773521Z');
SELECT * FROM @ts WHERE key <= Timestamp('2021-01-03T21:15:30.673521Z');

/* postgres can not */
/* multirun can not */
use plato;

insert into Output with truncate (a, b) values
('00', '10'),
('11', '20'),
('21', '30'),
('31', '40'),
('41', '50');

commit;

insert into Output select * from Output order by a;

commit;

select * from Output where a > '11'

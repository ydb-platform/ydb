--!syntax_pg
select count(*) from plato.concat('Input','Input');
select count(*) from plato.concat_view('Input','raw','Input','raw');
select count(*) from plato.range('');
select count(*) from plato.range('','A');
select count(*) from plato.range('','A','Z');
select count(*) from plato.range('','A','Z','');
select count(*) from plato.range('','A','Z','','raw');
select count(*) from plato.regexp('','Inpu.?');
select count(*) from plato.regexp('','Inpu.?','');
select count(*) from plato.regexp('','Inpu.?','','raw');
select count(*) from plato.like('','Inpu%');
select count(*) from plato.like('','Inpu%','');
select count(*) from plato.like('','Inpu%','','raw');


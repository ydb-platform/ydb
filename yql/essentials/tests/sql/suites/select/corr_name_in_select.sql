select b.sortkey from (select a.key as sortkey from plato.Input as a) as b order by b.sortkey desc;

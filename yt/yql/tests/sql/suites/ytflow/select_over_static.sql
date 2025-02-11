use plato;

pragma Engine = "ytflow";

insert into Output
select value, codec from Input;

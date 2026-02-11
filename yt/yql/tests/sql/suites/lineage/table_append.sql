use plato;    

insert into Output    
SELECT key FROM Input;

insert into Output 
SELECT subkey as key FROM Input;


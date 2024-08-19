pragma dq.EnableSpillingNodes="None";
pragma BlockEngine='force';
select sum(key) from plato.Input group by key;

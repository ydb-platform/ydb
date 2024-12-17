/* postgres can not */
/* can not check this with postgres becouse order of columns is not specified here */
select key, (value || "ab"), (value || "a"), value from plato.Input order by key;
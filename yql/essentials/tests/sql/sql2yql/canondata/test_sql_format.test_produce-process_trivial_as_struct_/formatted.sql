/* postgres can not */
PROCESS plato.Input0
USING SimpleUdf::Echo(value) AS val;

## Parameterized prepared queries {#param-prepared-queries}

Parameterized prepared queries are saved as templates, where specially formatted placeholders are replaced with relevant parameter values each time the query is executed. Use parameterized queries to improve performance by reducing the need for frequent compilation and recompilation of queries that differ only in parameter values. The prepared query is stored in the session context.

Code snippet for parameterized prepared queries:
## Parameterized queries {#param-queries}

Querying data using parameters. This query execution option is preferable as it allows the server to reuse the query execution plan for subsequent calls and also protects from such vulnerabilities as [SQL Injection]{% if lang == "en" %}(https://en.wikipedia.org/wiki/SQL_injection){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Embedding_SQL code){% endif %}.


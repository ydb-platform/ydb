## Database {#database}

_A {{ ydb-short-name }} database ({{ ydb-short-name }} DB)_ is an isolated consistent set of data that is accessed via {{ ydb-short-name }}.

{{ ydb-short-name }}:

* Accepts network connections from clients and processes their queries to update and read data.
* Authenticates and authorizes clients.
* Provides transactional updates and consistent data reads.
* Provides DB load scalability and high DB availability for clients.
* Stores information about a data schema, processes queries to update it, checks the hosted data for compliance with the schema, and allows you to create data queries using schema elements, such as tables, fields, and indexes.
* Provides consistent data updates in indexes when data is updated in tables.
* Generates metrics that can be used to monitor the DB performance.

Resources for the {{ ydb-short-name }} database (CPU, RAM, nodes, and disk space) are allocated within a {{ ydb-short-name }} cluster.


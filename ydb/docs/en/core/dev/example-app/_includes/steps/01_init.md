## Initializing a database connection {#init}

To interact with {{ ydb-short-name }}, create instances of the driver, client, and session:

* The {{ ydb-short-name }} driver facilitates interaction between the app and {{ ydb-short-name }} nodes at the transport layer. It must be initialized before creating a client or session and must persist throughout the {{ ydb-short-name }} access lifecycle.
* The {{ ydb-short-name }} client operates on top of the {{ ydb-short-name }} driver and enables the handling of entities and transactions.
* The {{ ydb-short-name }} session, which is part of the {{ ydb-short-name }} client context, contains information about executed transactions and prepared queries.
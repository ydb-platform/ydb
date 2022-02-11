## Hierarchy of cluster elements {#hierarchy} {#domain} {#account} {#directory} {#dir}

To host databases in {{ ydb-short-name }}, use the following directory structure: ```/domain/account/directory/database```, where:

* *A domain* is the first-level element where all cluster resources are located. There can only be one domain per cluster.
* *An account* is a second-level element that defines the owner of a DB group.
* *A directory* is a third-level element set by the account owner for grouping their databases.
* *A database* is a fourth-level element that corresponds to the DB.

At the top hierarchy level, the cluster contains a single domain that hosts one or more accounts, while accounts host one or more directories that host databases.

Using the hierarchy of entities, you can grant access rights and get aggregate or filtered monitoring indicators for the elements of any hierarchy level.


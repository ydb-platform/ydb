## Hierarchy of cluster elements {#hierarchy} {#domain} {#account} {#directory} {#dir}

To host databases in {{ ydb-short-name }}, use the following directory structure: `/domain/account/directory/database`, where:

* _A domain_ is the first-level element where all cluster resources are located. There can only be one domain per cluster.
* _An account_ is a second-level element that defines the owner of a DB group.
* _A directory_ is a third-level element defined by an account owner for grouping his or her databases.
* _A database_ is a fourth-level element that corresponds to a DB.

At the top hierarchy level, the cluster contains a single domain that hosts one or more accounts, while accounts host one or more directories that host databases.

Using the hierarchy of entities, you can grant access rights and get aggregate or filtered monitoring indicators for the elements of any hierarchy level.


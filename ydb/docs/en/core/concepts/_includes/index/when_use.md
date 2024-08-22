## Use cases {#use-cases}

{{ ydb-short-name }} can be used as an alternative solution in the following cases:

* When using NoSQL systems, if strong data consistency is required.
* When using NoSQL systems, if you need to make transactional updates to data stored in different rows of one or more tables.
* In systems that need to process and store large amounts of data and allow for virtually unlimited horizontal scalability (using industrial clusters of 5000+ nodes, processing millions of RPS, and storing petabytes of data).
* In low-load systems, when supporting a separate DB instance would be a waste of money (consider using {{ ydb-short-name }} in serverless mode instead).
* In systems with unpredictable or seasonally fluctuating load (you can add/reduce computing resources on request and/or in serverless mode).
* In high-load systems that shard load across relational DB instances.
* When developing a new product with no reliable load forecast or with an expected high load beyond the capabilities of conventional relational databases.
* In projects where simultaneous handling of transactional and analytical workloads is required.
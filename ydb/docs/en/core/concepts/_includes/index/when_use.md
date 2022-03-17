## Use cases {#use-cases}

{{ ydb-short-name }} can be used as an alternative solution in the following cases:

* When using NoSQL systems if strong data consistency is required.
* When using NoSQL systems if you need to make transactional changes to data stored in different rows of one or more tables.
* In systems that need to process and store large amounts of data and allow for almost infinite horizontal scalability (using industrial clusters of 5000+ nodes, processing millions of RPS and storing petabytes of data).
* In systems with low load, when support for a separate DB instance will be a waste of money (consider using {{ ydb-short-name }} in serverless mode instead).
* In systems with unpredictable or seasonally fluctuating load (you can add/reduce computing resources on request and/or in serverless mode).
* In high-load systems that shard load across relational DB instances.
* When developing a new product that has no reliable forecast of expected load or is natively designed for an expected high load beyond the capabilities of traditional relational databases.

---
title: Serverless and Dedicated modes in YDB
description: "YDB databases can be created in two modes Dedicated and Serverless. Dedicated mode of operation assumes that resources for Tablet instances and for executing YQL queries are selected from resources explicitly allocated for the compute database. In the Serverless mode of operation, the YDB infrastructure determines how many computing resources need to be allocated to service the user base."
keywords:
  - ydb
  - serverless
  - dedicated
editable: false
---
# Serverless and Dedicated modes {{ ydb-short-name }}

You can create and use multiple {{ ydb-short-name }} databases. When creating a database, one of two operating modes is selected for each database: Serverless or Dedicated. The mode can't be changed later.

* _Serverless_: A DB that doesn't require you to configure, administer, or monitor load or manage resources. To create a database, you only need to enter a name, and you'll get the URL for the connection. Payment is charged for the execution of queries and the actual amount of stored data.

* _Dedicated_: You determine the computing resources that will be reserved for the database: CPU and RAM on the nodes, the number of nodes, and the storage size. You need to make sure there are sufficient resources to handle the load and add more when necessary. Payment is charged for dedicated resources per hour, regardless of their actual use.


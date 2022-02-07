---
title: Режимы работы Serverless и Dedicated в Yandex Database
description: "Базы YDB могут быть созданы в двух режимах Dedicated и Serverless. Режим работы Dedicated предполагает, что ресурсы на инстансы Таблеток и на выполнение YQL-запросов выбираются из явно выделенных для базы данных compute ресурсов. В Serverless режиме работы инфраструктура YDB определяет сколько вычислительных ресурсов необходимо выделить для обслуживания пользовательской базы."
keywords:
  - ydb
  - serverless
  - dedicated
editable: false
---
# Serverless and Dedicated modes {{ ydb-full-name }}

You can create and use multiple {{ ydb-short-name }} databases. When creating a database, one of two operating modes is selected for each database: Serverless or Dedicated. The mode can't be changed later.

* _Serverless_: A DB that doesn't require you to configure, administer, or monitor load or manage resources. To create a database, you only need to enter a name, and you'll get the URL for the connection. Payment is charged for the execution of queries and the actual amount of stored data.

* _Dedicated_: You determine the computing resources that will be reserved for the database: CPU and RAM on the nodes, the number of nodes, and the storage size. You need to make sure there are sufficient resources to handle the load and add more when necessary. Payment is charged for dedicated resources per hour, regardless of their actual use.


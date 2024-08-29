# Getting started with {{ ydb-short-name }} as an Application Developer / Software Engineer

First of all, you'll need to obtain access to a {{ ydb-short-name }} cluster. Follow the [quickstart instructions](../quickstart.md) to get a basic local instance. Later on, you can work with your DevOps team to [build a production-ready cluster](../devops/index.md) or leverage one of the cloud service providers that offer a managed {{ ydb-short-name }} service.

The second step is designing a data schema for an application you will build from scratch or adapt the schema of an existing application if you're migrating from another database management system. Work with your DBA on this, or refer to the documentation [for DBA](../dev/index.md) yourself if you don't have one on your team.

In parallel with designing the schema, you need to set up your development environment for interaction with {{ ydb-short-name }}. There are a few main aspects to it, explored below.

## Choosing API

Choose the {{ ydb-short-name }} API you want to use; there are several options:

- The recommended way for mainstream programming languages is using a [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md). They provide high-level APIs and implement best practices on working with {{ ydb-short-name }}. {{ ydb-short-name }} SDKs are available for several popular languages and strive for feature parity, but not all are feature-complete. Refer to the [SDK feature comparison table](../reference/ydb-sdk/feature-parity.md) to check if the SDK for the programming language you had in mind will fit your needs or to choose a programming language with better feature coverage if you're flexible.
- Alternatively, {{ ydb-short-name }} provides [PostgreSQL-compatible API](../postgresql/intro.md). It is intended to simplify migrating existing applications that have outgrown PostgreSQL. However, it is also useful for exotic programming languages that have a PostgreSQL client library but don't have a {{ ydb-short-name }} SDK. Refer to PostgreSQL compatibility documentation to check if its completeness will suit your needs.
- If you are interested in [{{ ydb-short-name }} topics](../concepts/topic.md) feature, it is worth noting that they also provide [Kafka-compatible API](../reference/kafka-api/index.md). Follow that link if this use case is relevant.
- As a last resort, {{ ydb-short-name }}'s native API is based on the [gRPC](https://grpc.io/) protocol, which has an ecosystem around it, including code generation of clients. [{{ ydb-short-name}}'s gRPC specs are hosted on GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/public/api/grpc) and you could leverage them in your application. The generated clients are low-level and will require extra work to handle aspects like retries and timeouts properly, so go this route only if other options above aren't possible and you know what you're doing.
  
## Install prerequisites

Choose the specific programming language you'll be using. [Install the respective {{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md) or [a PostgreSQL driver](https://wiki.postgresql.org/wiki/List_of_drivers) depending on the route you have chosen above.

Additionally, you'd want to set up at least one of the available ways to run ad-hoc queries for debugging purposes. Choose at least one according to your preferences:

* [YDB CLI](../reference/ydb-cli/install.md)
* [Embedded UI](../reference/embedded-ui/index.md)
* Any SQL IDE that supports [JDBC](https://github.com/ydb-platform/ydb-jdbc-driver)
* [psql](https://www.postgresql.org/docs/14/app-psql.html) or [pgAdmin](https://www.pgadmin.org/) for the PostgreSQL-compatible route.

## Start coding

### For {{ ydb-short-name }} SDK route

- Go through [YQL tutorial](yql-tutorial/index.md) to get familiar with {{ ydb-short-name }}'s SQL dialect.
- Explore [example applications](example-app/index.md) to see how working with SDK's looks like.
- Check out [SDK recipies](../recipes/ydb-sdk/index.md) for typical SDK use cases, which you can refer to later.
- Leverage your IDE capabilities to navigate the SDK code.
  
### For PostgreSQL-compatibility route

- Learn how to [connect PostgreSQL driver with {{ ydb-short-name }} cluster](../postgresql/docker-connect.md).
- The rest should be similar to using vanilla PostgreSQL. Use your experience with it or refer to any favorite resources. However, refer to the list of [functions](../postgresql/functions.md) and statements to adjust your expectations.

## Testing

To write tests on applications working with {{ ydb-short-name }}:

- For functional tests, you can mock {{ ydb-short-name }}'s responses using a suitable testing framework for your chosen programming language.
- For integrational tests, you can launch a single-node {{ ydb-short-name }} instance in your [CI/CD](https://en.wikipedia.org/wiki/CI/CD) environment with either a Docker image or executable similarly to how it is done in the [Quickstart article](../quickstart.md). It is recommended to test against a few versions of {{ ydb-short-name }}: the one you have in production to check for issues you can encounter when updating your application and the newer versions to identify the potential issues of upgrading {{ ydb-short-name }} early on.
- For performance tests, you'd want to use a cluster deployed according to instructions for [production use](../devops/index.md) as single-node won't yield realistic results. You can run [ydb workload](../reference/ydb-cli/commands/workload/index.md) if you want to see how your {{ ydb-short-name }} cluster performs in generic scenarios even before writing any application code. Then you can use the [source code of the library behind this tool](https://github.com/ydb-platform/ydb/tree/main/ydb/library/workload) as an example of how to write your own performance tests with {{ ydb-short-name }}. It'd be great if you could [contribute](../contributor/index.md) an anonymized version of your workload to upstream so it can be included in performance testing of {{ ydb-short-name }} itself.

## What's next

The above should be enough to start developing applications that interact with {{ ydb-short-name }}. Along the way use [YQL](../yql/reference/index.md) and [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md) reference documentation and other resources in this documentation section.

In case of any issues, feel free to discuss them in [{{ ydb-short-name }} Discord](https://discord.gg/R5MvZTESWc).
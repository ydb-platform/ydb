# Data transfer

Transfer in {{ ydb-short-name }} is an asynchronous mechanism for moving data from a [topic](glossary.md#topic) to a table. Creating a transfer instance, modifying it, and deleting it is done using YQL. The transfer runs inside the database and works in the background. Transfer is used to solve the task of delivering data from a topic to a table.

In practice, it's often more convenient to write data not directly to a table, but to a topic, and then asynchronously rewrite it from the topic to the table. This approach allows for even load distribution and handling spikes, since writing to a message queue is a lighter operation. Depending on the number of messages written to the topic, the delay in data availability for reading from the table after adding to the topic can range from several seconds to several minutes.

The transfer reads messages from the specified topic, transforms them into a format suitable for writing to the table, and then writes them to the target table. If the table already contains a row with the specified [primary key](glossary.md#primary-key), the row will be updated. Data transformation is performed using a [lambda function](../yql/reference/syntax/expressions.md#lambda) that takes a message as a parameter and returns a list of [structures](../yql/reference/types/containers.md), each corresponding to a row to be added or replaced in the table. For example, if the table contains a column `example_column` of type `Int32`, the structure must include a named field `example_column` of the same type.
If the [lambda function](../yql/reference/syntax/expressions.md#lambda) returns an empty list, no rows in the table will be changed. The lambda function is not allowed to access tables and other {{ ydb-short-name }} objects.
The lambda function being used can be obtained from the [description](../reference/ydb-cli/commands/scheme-describe.md) of the transfer instance.

Transfer consumes additional server resources, primarily CPU. CPU consumption depends on the complexity of transforming the data written to the topic.

Transfer can read data from topics located both in the [database](glossary.md#database) where it's created and in another {{ ydb-short-name }} database or [cluster](glossary.md#cluster). To read a topic from another database when creating a transfer, you must specify the connection parameters to that database. The target table must be located in the same database where the transfer is created.

Reading from a [topic](glossary.md#topic) is performed using a [consumer](glossary.md#consumer). The transfer automatically adds a consumer with a unique name to the topic for reading messages. When the transfer is deleted, the created consumer is automatically deleted. It's also possible to create a consumer manually and specify its name when creating the transfer. A manually created consumer is not automatically deleted when the transfer is deleted. The name of the automatically created consumer can be obtained from the [description](../reference/ydb-cli/commands/scheme-describe.md) of the transfer instance.

If data needs to be transferred from one topic to multiple tables, a separate transfer must be created for each table. Similarly, data from multiple topics can be directed to one table, for which each topic must be associated with a separate transfer. If the consumer name is set manually, then when there are multiple transfers reading from one topic, each transfer must be assigned a unique consumer, otherwise the data will be processed by only one of them.

{% note info %}

When updating a row in the table, all row values are overwritten with the data from the structure returned by the lambda function. If the structure doesn't contain a named field corresponding to a table column, the value of that column will be set to `NULL`. Named fields that don't have a correspondence in the table are ignored.

When attempting to write a `NULL` value to a `NOT NULL` table column, the transfer ends with a [critical error](#error-handling).

{% endnote %}

## Guarantees {#guarantees}

* Table writes are guaranteed to follow the write order in the topic [partition](glossary.md#partition). It's recommended to group messages related to one table row into one topic partition.
* Writing to the table is performed using [bulk upsert](../recipes/ydb-sdk/bulk-upsert.md) without atomicity guarantees. Data writing is split into several independent transactions, each affecting a single table partition, with parallel execution.

## Transfer startup

If a transfer is created without explicitly specifying a consumer name, a new consumer will be added to the topic. In this case processing of topic messages will start from the first message in the topic.
If a transfer is created with a previously created consumer specified, processing of topic messages will start from the first message not processed by that consumer.

## Required permissions for transfer operation {#permissions}

To create and execute a transfer, the user must have write permissions to the target table and read permissions from the source topic. If the topic is located in another {{ ydb-short-name }} database and the consumer is created automatically, additional permission to modify the topic is required, which allows the transfer to automatically create the consumer and delete it when the transfer is deleted.

## Diagnostics

Current transfer parameters, including the lambda function text, can be viewed in the [Embedded UI](../reference/embedded-ui/index.md) and in the [description](../reference/ydb-cli/commands/scheme-describe.md) of the transfer instance.

Data processing speed and delays can be monitored using [consumer metrics](../reference/observability/metrics/index.md#topics) that are used for reading from the topic.

## Temporary transfer suspension {#pause-and-resume}

Transfer operation can be temporarily paused and then resumed. After resuming transfer operation, messages following the last processed message in the topic will start being processed.

To pause the transfer, you should change the transfer status to `PAUSED`. To resume the transfer, change the status to `ACTIVE`.

{% note warning %}

The topic has a [message retention time](datamodel/topic.md#retention-time), after which messages are deleted. If the transfer is paused longer than the retention time, the messages are deleted and won’t be processed when the transfer is resumed.

To guarantee that unprocessed messages are not deleted, you should make the consumer [important](datamodel/topic.md#important-consumer).

{% endnote %}

## Error handling during transfer {#error-handling}

Different types of errors can occur during the transfer process:

* **Temporary failures**. Transport errors, system overload, and other temporary problems. Requests will be retried until successful execution.
* **Critical errors**. Errors related to access rights, data schema, and other critical aspects. When such errors occur, the transfer will be stopped, and the error text will be displayed on the transfer page in the [Embedded UI](../reference/embedded-ui/index.md) user interface. The error text can also be obtained from the [description](../reference/ydb-cli/commands/scheme-describe.md) of the transfer instance.

To resume a transfer operation, eliminate the cause of the error and execute the `ALTER TRANSFER` command. For example, if the error was in the lambda function, change the lambda function. If the error is not related to the transfer configuration, for example, missing read permissions, then after eliminating the cause of the error, the transfer must be restarted by [temporarily stopping](#pause-and-resume) and then [resuming](#pause-and-resume) its operation.
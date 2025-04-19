# Message pipeline processing

The use of the `topic read` and `topic write` commands with standard I/O devices and support for reading messages in streaming mode lets you build full-featured integration scenarios with message transfer across topics and their conversion. This section describes a number of these scenarios.

* Transferring a single message from `topic1` in the `quickstart` database to `topic2` in `db2`, waiting for it to appear in the source topic

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 -w | {{ ydb-cli }} -p db2 topic write topic2
   ```

* Transferring all one-line messages that appear in `topic1` in the `quickstart` database to `topic2` in `db2` in background mode. You can use this scenario if it's guaranteed that there are no `0x0A` bytes (newline) in source messages.

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
   {{ ydb-cli }} -p db2 topic write topic2 --format newline-delimited
   ```

* Transferring an exact binary copy of all messages that appear in `topic1` in the `quickstart` database to `topic2` in `db2` in background mode with base64-encoding of messages in the transfer stream.

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w --transform base64 | \
   {{ ydb-cli }} -p quickstart topic write topic2 --format newline-delimited --transform base64
   ```

* Transferring a limited batch of one-line messages filtered by the `ERROR` substring

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited | \
   grep ERROR | \
   {{ ydb-cli }} -p db2 topic write topic2 --format newline-delimited
   ```

* Writing YQL query results as messages to `topic1`

   ```bash
   {{ ydb-cli }} -p quickstart yql -s "select * from series" --format json-unicode | \
   {{ ydb-cli }} -p quickstart topic write topic1 --format newline-delimited
   ```

## Running of an SQL query with the transmission of messages from the topic as parameters {#example-read-to-yql-param}

* Running a YQL, passing each message read from `topic1` as a parameter

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
   {{ ydb-cli }} -p quickstart table query execute -q 'declare $s as String;select Len($s) as Bytes' \
   --stdin-format newline-delimited --stdin-par s --stdin-format raw
   ```

* Running a YQL query involving adaptive batching of parameters from messages read from `topic1`

   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
   {{ ydb-cli }} -p quickstart table query execute \
   -q 'declare $s as List<String>;select ListLength($s) as Count, $s as Items' \
   --stdin-format newline-delimited --stdin-par s --stdin-format raw \
   --batch adaptive
   ```

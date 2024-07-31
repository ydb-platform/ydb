# Reading messages from a topic

The `topic read` command reads messages from a topic and outputs them to a file or the command-line terminal:

```bash
{{ ydb-cli }} [connection options] topic read <topic-path> --consumer STR \
  [--format STR] [--wait] [--limit INT] \
  [--transform STR] [--file STR] [--commit BOOL] \
  [additional parameters...]
```

{% include [conn_options_ref.md](commands/_includes/conn_options_ref.md) %}

Three command modes are supported:

1. **Single message**. No more than one message is read from a topic.
2. **Batch mode**. Messages are read from a topic until it runs out of messages for processing or their number exceeds the limit that must be set.
3. **Streaming mode**. Messages are read from a topic as they appear while waiting for new messages to arrive until you terminate the command with `Ctrl+C` or the number of messages exceeds the limit that is set optionally.

## Parameters {#options}

### Required parameters

| Name | Description |
---|---
| `<topic-path>` | Topic path |
| `-c VAL`, `--consumer VAL` | Topic consumer name.<br/>Message consumption starts from the current offset for this consumer (if the `--timestamp` parameter is not specified).<br/>The current offset is shifted as messages are consumed and output (if `--commit=false` is not set). |

### Basic optional parameters

`--format STR`: Output format.

- Specifies how to format messages at the output. Some formats don't support streaming mode.
- List of supported formats:

   | Name | Description | Is<br/>streaming mode supported? |
   ---|---|---
   | `single-message`<br/>(default) | The contents of no more than one message are output without formatting. | - |
   | `pretty` | Output to a pseudo-graphic table with columns containing message metadata. The message itself is output to the `body` column. | No |
   | `newline-delimited` | Messages are output with a delimiter (`0x0A` newline character) added after each message. | Yes |
   | `concatenated` | Messages are output one after another with no delimiter added. | Yes |

`--wait` (`-w`): Waiting for new messages to arrive.

- Enables waiting for the first message to appear in a topic. If not set and the topic has no messages to handle, the command is terminated once started. If the flag is set, the started read message command waits for the first message to arrive to be processed.
- Enables streaming selection mode for the formats that support it, or else batch mode is used.

`--limit INT`: The maximum number of messages that can be consumed from a topic.

- The default and acceptable values depend on the selected output format:

   | Does the format<br/>support streaming selection mode? | Default limit value | Acceptable values |
   ---|---|---
   | No | 10 | 1-500 |
   | Yes | 0 (no limit) | 0-500 |

`--transform VAL`: Method for transforming messages.

- Defaults to `none`.
- Possible values:
   `base64`: A message is transformed into [Base64](https://ru.wikipedia.org/wiki/Base64)
   `none`: The contents of a message are output byte by byte without transforming them.

`--file VAL` (`-f VAL`): Write the messages read to the specified file. If not set, messages are output to `stdout`.

`--commit BOOL`: Commit message reads.

1. If `true` (by default), a consumer's current offset is shifted as topic messages are consumed.
2. Possible values: `true` or `false`.

### Other optional parameters

| Name | Description |
---|---
| `--idle-timeout VAL` | Timeout for deciding if a topic is empty, meaning that it contains no messages for processing. <br/>The time is counted from the point when a connection is established once the command is run or when the last message is received. If no new messages arrive from the server during the specified timeout, the topic is considered to be empty.<br/>Defaults to `1s` (1 second). |
| `--timestamp VAL` | Message consumption starts from the point in time specified in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format.<br/>If not set, messages are consumed starting from the consumer's current offset in the topic.<br/>If set, consumption starts from the first [message](../../concepts/topic.md#message) received after the specified time. |
| `--with-metadata-fields VAL` | List of [message attributes](../../concepts/topic.md#message) whose values should be output in columns with metadata in `pretty` format. If not set, columns with all attributes are output. <br/>Possible values:<ul><li>`write_time`: The time a message is written to the server in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format.</li><li>`meta`: Message metadata.</li><li>`create_time`: The time a message is created by the source in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format.</li><li>`seq_no`: Message [sequence number](../../concepts/topic.md#seqno).</li><li>`offset`: [Message sequence number within a partition](../../concepts/topic.md#offset).</li><li>`message_group_id`: [Message group ID](../../concepts/topic.md#producer-id).</li><li>`body`: Message body.</li></ul> |
| `--partition-ids VAL` | Comma-separated list of [partition](../../concepts/topic.md#partitioning) identifiers to read from.<br/>If not specified, messages are read from all partitions. |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

In all the examples below, a topic named `topic1` and a consumer named `c1` are used.

* Reading a single message with output to the terminal: If the topic doesn't contain new messages for this consumer, the command terminates with no data output:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1
   ```

* Waiting for and reading a single message written to a file named `message.bin`. The command keeps running until new messages appear in the topic for this consumer. However, you can terminate it with `Ctrl+C`:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 -w -f message.bin
   ```

* Viewing information about messages waiting to be handled by the consumer without committing them. Up to 10 first messages are output:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format pretty --commit false
   ```

* Output messages to the terminal as they appear, using newline delimiter characters and transforming messages into Base64. The command will be running until you terminate it with `Ctrl+C`:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 -w --format newline-delimited --transform base64
   ```

* Track when new messages with the `ERROR` text appear in the topic and output them to the terminal once they arrive:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | grep ERROR
   ```

* Receive another non-empty batch of no more than 150 messages transformed into base64, delimited with newline characters, and written to the `batch.txt` file:
   ```bash
   {{ ydb-cli }} -p quickstart topic read topic1 -c c1 \
     --format newline-delimited -w --limit 150 \
     --transform base64 -f batch.txt
   ```

* [Examples of YDB CLI command integration](topic-pipeline.md)

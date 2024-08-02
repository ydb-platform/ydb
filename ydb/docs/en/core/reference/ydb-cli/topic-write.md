# Writing messages to a topic

The `topic write` command writes messages to a topic from a file or `stdin`:

```bash
{{ ydb-cli }} [connection options] topic write <topic-path> \
  [--file STR] [--format STR] [--transform STR] \
  [additional parameters...]
```

{% include [conn_options_ref.md](commands/_includes/conn_options_ref.md) %}

## Parameters {#options}

### Basic parameters

`<topic-path>`: Topic path, the only required parameter.

`--file VAL` (`-f VAL`): Read a stream of incoming messages and write them to a topic from the specified file. If not set, messages are read from `stdin`.

`--format STR`: Format of the incoming message stream.
* Supported formats

   | Name | Description |
   ---|---
   | `single-message`<br/>(default) | The entire input stream is treated as a single message to be written to the topic. |
   | `newline-delimited` | A stream at the input contains multiple messages delimited with the `0x0A` newline character. |

`--transform VAL`: Method for transforming messages.

- Defaults to `none`.
- Possible values:
   `base64`: Decode each message in the input stream from [Base64](https://ru.wikipedia.org/wiki/Base64) and write the output to the topic. If decoding fails, the command is aborted with an error.
   `none`: Write the contents of a message from the input stream to the topic byte by byte without transforming them.

### Additional parameters

| Name | Description |
---|---
| `--delimiter STR` | Delimiter byte. The input stream is delimited into messages with the specified byte. Specified only if no `--format` is set. Specified as an escaped string. |
| `--message-group-id STR` | Message group string ID. If not set, all messages generated from the input stream are assigned the same ID value as a hexadecimal string representation of a random three-byte integer. |
| `--codec STR` | Codec used for message compression on the client before sending them to the server. Possible values: `RAW` (no compression, default), `GZIP`, and `ZSTD`. Compression causes higher CPU utilization on the client when reading and writing messages, but usually lets you reduce the volume of data transferred over the network and stored. When consumers read messages, they're automatically decompressed with the codec used when writing them, without specifying any special options. Make sure the specified codec is listed in the [topic parameters](topic-create.md#create-options) as supported. |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

All the examples given below use a topic named `topic1`.

* Writing a terminal input to a single message Once the command is run, you can type any multi-line text and press `Ctrl+D` to input it.
   ```bash
   {{ ydb-cli }} -p quickstart topic write topic1
   ```

* Writing the contents of the `message.bin` file to a single message compressed with the GZIP codec
   ```bash
   {{ ydb-cli }} -p quickstart topic write topic1 -f message.bin --codec GZIP
   ```

* Writing the contents of the `example.txt` file delimited into messages line by line
   ```bash
   {{ ydb-cli }} -p quickstart topic write topic1 -f example.txt --format newline-delimited
   ```

* Writing a resource downloaded via HTTP and delimited into messages with tab characters
   ```bash
   curl http://example.com/resource | {{ ydb-cli }} -p quickstart topic write topic1 --delimiter "\t"
   ```

* [Examples of YDB CLI command integration](topic-pipeline.md)

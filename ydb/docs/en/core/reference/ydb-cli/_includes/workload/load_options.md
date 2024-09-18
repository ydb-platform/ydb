### Common parameters of the import command {#load_options}

Option name | Option description
---|---
`--upload-threads <value>` or `-t <value>` | The number of execution threads for preparing data. By default, it corresponds to the number of available cores on the client machine.
`--bulk-size <value>` | The size of the chunk for sending data in rows. By default, 10000.
`--max-in-flight <value>` | The maximum number of chunks of data that are simultaneously being processed. By default, 128.

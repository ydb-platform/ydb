### Common parameters of the import command {#load_options}

Name | Description | Default value
---|---|---
`--upload-threads <value>` or `-t <value>` | The number of execution threads for data preparation. | The number of available cores on the client.
`--bulk-size <value>` | The size of the chunk for sending data, in rows. | 10000
`--max-in-flight <value>` | The maximum number of data chunks that can be processed simultaneously. | 128
`--file-output-path <value>` or `-f <path>` | If this option is set, the data will not be loaded into the database, but will be saved to the directory <path>. |

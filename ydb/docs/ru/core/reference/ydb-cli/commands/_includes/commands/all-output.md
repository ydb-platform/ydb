```text
YDB client

Usage: ydb [options...] <subcommand>

Subcommands:
ydb
├─ config                   Manage YDB CLI configuration
│  └─ profile               Manage configuration profiles
│     ├─ activate           Activate specified configuration profile (aliases: set)
│     ├─ create             Create new configuration profile or re-configure existing one
│     ├─ delete             Delete specified configuration profile (aliases: remove)
│     ├─ get                List values for specified configuration profile
│     └─ list               List configuration profiles
├─ discovery                Discovery service operations
│  ├─ list                  List endpoints
│  └─ whoami                Who am I?
├─ export                   Export service operations
│  └─ s3                    Create export to S3
├─ import                   Import service operations
│  ├─ file                  Import data from file
│  │  ├─ csv                Import data from CSV file
│  │  └─ tsv                Import data from TSV file
│  └─ s3                    Create import from S3
├─ init                     YDB CLI initialization
├─ operation                Operation service operations
│  ├─ cancel                Start cancellation of a long-running operation
│  ├─ forget                Forget long-running operation
│  ├─ get                   Check status for a given operation
│  └─ list                  List operations of specified kind
├─ scheme                   Scheme service operations
│  ├─ describe              Show information about object at given object
│  ├─ ls                    Show information about objects inside given directory
│  ├─ mkdir                 Make directory
│  ├─ permissions           Modify permissions
│  │  ├─ chown              Change owner
│  │  ├─ clear              Clear permissions
│  │  ├─ grant              Grant permission (aliases: add)
│  │  ├─ revoke             Revoke permission (aliases: remove)
│  │  └─ set                Set permissions
│  └─ rmdir                 Remove directory
├─ scripting                Scripting service operations
│  └─ yql                   Execute YQL script
├─ table                    Table service operations
│  ├─ attribute             Attribute operations (aliases: attr)
│  │  ├─ add                Add attributes to the specified table
│  │  └─ drop               Drop attributes from the specified table
│  ├─ drop                  Drop a table
│  ├─ index                 Index operations
│  │  ├─ add                Add index in to the specified table
│  │  │  ├─ global-async    Add global async index. The command returns operation
│  │  │  └─ global-sync     Add global sync index. The command returns operation (aliases: global)
│  │  └─ drop               Drop index from the specified table
│  ├─ query                 Query operations
│  │  ├─ execute            Execute query (aliases: exec)
│  │  └─ explain            Explain query
│  ├─ readtable             Stream read table
│  └─ ttl                   Ttl operations
│     ├─ drop               Drop ttl settings from the specified table
│     └─ set                Set ttl settings for the specified table
├─ tools                    YDB tools service
│  ├─ copy                  Copy table(s)
│  ├─ dump                  Dump specified database directory or table into local directory
│  ├─ rename                Rename or repalce table(s)
│  └─ restore               Restore database from local dump into specified directory
├─ update                   Update current YDB CLI binary if there is a newer version available
├─ version                  Print Yandex.Cloud YDB CLI version
└─ yql                      Execute YQL script (streaming)


Options:
  {-?|-h|--help}        Print usage
  {-e|--endpoint} [PROTOCOL://]HOST[:PORT]
                        [Required] Endpoint to connect. Protocols: grpc, grpcs (Default: grpcs).
                          Endpoint search order:
                            1. This option
                            2. Profile specified with --profile option
                            3. Active configuration profile
  {-d|--database} PATH  [Required] Database to work with.
                          Database search order:
                            1. This option
                            2. Profile specified with --profile option
                            3. Active configuration profile
  {-v|--verbose}        Increase verbosity of operations (default: 0)
  --ca-file PATH        Path to a file containing the PEM encoding of the server root certificates for tls connections.
                        If this parameter is empty, the default roots will be used.
  --iam-token-file PATH IAM token file. Note: IAM tokens expire in 12 hours.
                          For more info go to: cloud.yandex.ru/docs/iam/concepts/authorization/iam-token
                          Token search order:
                            1. This option
                            2. Profile specified with --profile option
                            3. "IAM_TOKEN" environment variable
                            4. Active configuration profile
  --yc-token-file PATH  YC token file. It should contain OAuth token of a Yandex Passport user to get IAM token with.
                          For more info go to: cloud.yandex.ru/docs/iam/concepts/authorization/oauth-token
                          Token search order:
                            1. This option
                            2. Profile specified with --profile option
                            3. "YC_TOKEN" environment variable
                            4. Active configuration profile
  --use-metadata-credentials
                        Use metadata service on a virtual machine to get credentials
                          For more info go to: cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm
                          Definition priority:
                            1. This option
                            2. Profile specified with --profile option
                            3. "USE_METADATA_CREDENTIALS" environment variable
                            4. Active configuration profile (default: 0)
  --sa-key-file PATH    Service account key file
                          For more info go to: cloud.yandex.ru/docs/iam/operations/iam-token/create-for-sa
                          Definition priority:
                            1. This option
                            2. Profile specified with --profile option
                            3. "SA_KEY_FILE" environment variable
                            4. Active configuration profile
  --iam-endpoint STR    Endpoint of IAM service (default: "iam.api.cloud.yandex.net")
  --profile NAME        Profile name to use configuration parameters from.
  --license             Print license
  --credits             Print third-party licenses

Free args: min: 1, max: unlimited
  <subcommand>  config,discovery,export,import,init,operation,scheme,scripting,table,tools,update,version,yql
```

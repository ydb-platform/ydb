# TLS certificate generation script for YDB

In order to simplify generation and re-generation of certificates for YDB cluster, the `ydb-ca-update.sh` script has been created.

The recommended option is to generate a separate certificate for each cluster node. Users may choose to generate a single wildcard certificate for the whole cluster instead, by specifying the host name in the form of `*.domain.com`.

The script reads the list of certificate host names from `ydb-ca-nodes.txt` file, one hostname per line. Host names should be specified exactly as they are defined in the YDB cluster configuration file. If the wildcard name is used, it should match the correspoding hosts DNS names. Up to two host names can be specified in each line, both referring to the same host.

The generated certificates are written into the directory structure in the `CA` subdirectory, which is created if missing.

In case the certificate authority is not initialized yet, private CA key and certificate are generated.

For each host name or wildcard listed in the `ydb-ca-nodes.txt` file, each invocation of the script generates the new key and new certificate signed by the private CA. All generated files are put into `CA/certs/YYYY-MM-DD_hh-mi-ss` subdirectory.

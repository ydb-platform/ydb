# JDBC URL examples

- Local Docker container with anonymous authentication and without TLS:<br/>`jdbc:ydb:grpc://localhost:{{ ydb-ports.grpc }}/local`
- Remote self-hosted cluster:<br/>`jdbc:ydb:grpcs://<host>:{{ ydb-ports.grpcs }}/Root/<testdb>?secureConnectionCertificate=file:~/<myca>.cer`
- A cloud database instance with a token:<br/>`jdbc:ydb:grpcs://<host>:{{ ydb-ports.grpcs }}/<path/to/database>?token=file:~/my_token`
- A cloud database instance with a service account:<br/>`jdbc:ydb:grpcs://<host>:{{ ydb-ports.grpcs }}/<path/to/database>?saFile=file:~/sa_key.json`
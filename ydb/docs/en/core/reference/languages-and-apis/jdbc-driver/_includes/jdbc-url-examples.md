# JDBC URL Examples

- Local or remote Docker (anonymous authentication):<br>`jdbc:ydb:grpc://localhost:2136/local`
- Self-hosted cluster:<br>`jdbc:ydb:grpcs://<host>:2135/Root/testdb?secureConnectionCertificate=file:~/myca.cer`
- Connect with token to the cloud instance:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?token=file:~/my_token`
- Connect with service account to the cloud instance:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?saFile=file:~/sa_key.json`
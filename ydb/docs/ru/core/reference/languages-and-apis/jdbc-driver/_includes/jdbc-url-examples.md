# Примеры JDBC URL

- Локальный или удаленный Docker (анонимная аутентификация):<br>`jdbc:ydb:grpc://localhost:2136/local`
- Кластер, размещенный на собственном сервере:<br>`jdbc:ydb:grpcs://<host>:2135/Root/testdb?secureConnectionCertificate=file:~/myca.cer`
- С помощью токена:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?token=file:~/my_token`
- С помощью файла сервисного аккаунта:<br>`jdbc:ydb:grpcs://<host>:2135/path/to/database?saFile=file:~/sa_key.json`
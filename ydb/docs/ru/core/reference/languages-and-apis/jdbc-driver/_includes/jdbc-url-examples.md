# Примеры JDBC URL

- Локальный Docker контейнер с анонимной аутентификацией и без TLS:<br/>`jdbc:ydb:grpc://localhost:2136/local`
- Удаленный кластер, размещенный на собственном сервере:<br/>`jdbc:ydb:grpcs://<host>:2135/Root/<testdb>?secureConnectionCertificate=file:~/<myca>.cer`
- Экземпляр облачной базы данных с токеном:<br/>`jdbc:ydb:grpcs://<host>:2135/<path/to/database>?token=file:~/my_token`
- Экземпляр облачной базы данных с файлом сервисного аккаунта:<br/>`jdbc:ydb:grpcs://<host>:2135/<path/to/database>?saFile=file:~/sa_key.json`
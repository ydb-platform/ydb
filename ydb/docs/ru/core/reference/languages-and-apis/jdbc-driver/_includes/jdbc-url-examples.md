# Примеры JDBC URL

- Локальный Docker контейнер с анонимной аутентификацией и без TLS:<br/>`jdbc:ydb:grpc://localhost:{{ def-ports.grpc }}/local`
- Удаленный кластер, размещенный на собственном сервере:<br/>`jdbc:ydb:grpcs://<host>:{{ def-ports.grpcs }}/Root/<testdb>?secureConnectionCertificate=file:~/<myca>.cer`
- Экземпляр облачной базы данных с токеном:<br/>`jdbc:ydb:grpcs://<host>:{{ def-ports.grpcs }}/<path/to/database>?token=file:~/my_token`
- Экземпляр облачной базы данных с файлом сервисного аккаунта:<br/>`jdbc:ydb:grpcs://<host>:{{ def-ports.grpcs }}/<path/to/database>?saFile=file:~/sa_key.json`
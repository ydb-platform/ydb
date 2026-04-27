# Аутентификация Kafka API

## Включение аутентификации

При [самостоятельном развертывании YDB](../../quickstart.md) по умолчанию используется [анонимная аутентификация](../../security/authentication.md#anonymous)
, не требующая логина-пароля.

Чтобы включить обязательную аутентификацию, следуйте инструкции в статье [Аутентификация](../../security/authentication.md#static-credentials).

Аутентификация всегда включена при использовании [Kafka API в Yandex Cloud](https://yandex.cloud/ru/docs/data-streams/kafkaapi/auth)

## Механизмы аутентификации

В Kafka API поддержано два механизма SASL аутентификации: `PLAIN` и `SCRAM-SHA-256`, а также mTLS аутентификация.

### Аутентификация с помощью PLAIN и SCRAM-SHA-256

Оба механизма могут осуществляться как внутри протокола `TLS`, так и вне, порождая соответственно комбинации:

* `SASL_PLAINTEXT/PLAIN`;
* `SASL_SSL/PLAIN`;
* `SASL_PLAINTEXT/SCRAM-SHA-256`;
* `SASL_SSL/SCRAM-SHA-256`.

{% note warning %}

Для использования механизма `SCRAM-SHA-256` при аутентификации существующих пользователей может потребоваться смена пароля.

{% endnote %}

Для аутентификации необходимы:

* `<user-name>` — имя пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<password>` — пароль пользователя. Об управлении пользователями читайте в разделе [{#T}](../../security/authentication.md).
* `<database>` — путь к [базе данных](../../concepts/connect#database), с которой предполагается дальнейшее взаимодействие. (Необходим только для механизма `PLAIN`).

Для механизма `SCRAM-SHA-256` база данных определяется на основе настроек подключения [Kafka Connect](./connect/connect-step-by-step.md).
Целевой базой данных считается та база, к которой относится [узел базы данных](../../concepts/glossary#database-node), имеющий указанный `<ydb-endpoint>`.

Из этих параметров формируются следующие переменные, которые вы можете использовать в
`sasl.jaas.config` параметре конфигурации клиента Kafka:

* `<sasl.username>` = `<user-name>[@<database>]`
* `<sasl.password>` = `<password>`

{% note warning %}

Обратите внимание, логика формирования `<sasl.username>` и `<sasl.password>` в облачных инсталляциях {{ ydb-short-name }} может отличаться от приведенной здесь.

{% endnote %}

Примеры аутентификации смотрите в [Чтение и запись](./examples.md).

### Аутентификация по mTLS

Чтобы kafka клиент мог аутентифицироваться по mTLS, нужно выполнить следующие шаги.

#### Создание серверного и клиентского сертификатов

Для каждого шага ниже представлены примеры команд. Замените \*\*\* на ваши значения.

1. Создайте Certificate Authority (CA)

```bash
openssl genrsa -out ca-key.pem 4096
```

```bash
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem -subj "/C=***/ST=***/L=***/O=***/CN=MyKafkaRootCA"
```

2. Создайте сертификат для сервера

```bash
openssl genrsa -out server-key.pem 4096
```

В следующей команде вместо `serverhost.com` также укажите название вашего хоста.

```bash
openssl req -new -key server-key.pem -out server-cert.csr -subj "/C=***/ST=***/L=***/O=***/CN=serverhost.com"
```

```bash
cat > server-ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = DNS:serverhost.com
EOF
```

```bash
openssl x509 -req -in server-cert.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -days 365 -extfile server-ext.cnf
```

3. Создайте клиентский сертификат

```bash
openssl genrsa -out client-key.pem 4096
```

Замените `clienthost.com` на hostname вашего клиента.

```bash
openssl req -new -key client-key.pem -out client-cert.csr -subj "/C=***/ST=***/L=***/O=***/CN=clienthost.com"
```

```bash
cat > client-ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = DNS:clienthost.com
EOF
```

```bash
openssl x509 -req -in client-cert.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -days 365 -extfile client-ext.cnf
```


4. Добавьте сертификаты в keystore и truststore

Для сервера:

```bash
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem -out server.p12 -name kafka-server -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore server.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-server 

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore server.truststore.jks -storepass changeit -noprompt
```

Для клиента:

```bash
openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -out client.p12 -name kafka-client -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-client  

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore client.truststore.jks -storepass changeit -noprompt  
```

После этих пунктов у вас должны появиться нужные keystore и truststore, а также файлы с сертификатами и ключами.

#### Конфигурация клиента
##### Пример для Java SDK
```java
props.put("security.protocol", "SSL");
props.put("ssl.truststore.password", "changeit");
props.put("ssl.truststore.location", "/full/path/to/client.truststore.jks");
props.put("ssl.keystore.location", "/full/path/to/client.keystore.jks");
props.put("ssl.keystore.password", "changeit");
props.put("ssl.key.password", "changeit");
props.put("ssl.endpoint.identification.algorithm", "");
```

##### Пример для kafka cli

```
security.protocol=SSL
ssl.truststore.password=changeit
ssl.truststore.location=/full/path/to/client.truststore.jks
ssl.keystore.location=/full/path/to/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.endpoint.identification.algorithm=
```

#### Конфигурация YDB

Во-первых, нужно указать нужные поля в конфигурации kafka\_proxy

```yaml
kafka_proxy_config:
  enable_kafka_proxy: true
  listening_port: your_port

  mtls_enable: true
  key: "server-key.pem" # укажите правильные пути до файлов
  cert: "server-cert.pem"
  ca: "ca-cert.pem"
  enable_self_signed_certs: true # разрешаете ли вы самоподписанные сертификаты
```

Также укажите в конфигурации client\_certificate\_authorization правила, по которым будет проходить аутентификация:

```yaml
client_certificate_authorization:
  client_certificate_definitions:
    - require_same_issuer: true
      subject_terms:
        - short_name: CN
          suffixes:
            - '.myhost.net' # нужно заменить на нужный суффикс
      member_groups:
        - user@cert # заменить на нужную member группу
  request_client_certificate: true
```
Подробнее про client_certificate_authorization конфигурацию: [{#T}](../configuration/client_certificate_authorization.md)

Для корректной работы нужно использовать тот же сертификат, что и в настройках grpc, поэтому нужно указать путь до этого же серверного сертификата в конфигурации grpc.

Сейчас настроить конфигурации kafka и grpc с разными серверными сертификатами или указать серверный сертификат только в настройках kafka_proxy_config при использовании mtls нельзя.
```yaml
grpc_config:
  cert: "/path/to/server-cert.pem"
```

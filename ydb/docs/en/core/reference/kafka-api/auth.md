# Kafka API authentication

## Enabling authentication

When you run [a single-node local {{ ydb-short-name }} cluster](../../quickstart.md), [anonymous authentication](../../security/authentication.md#anonymous) is used by default. It doesn't require a username and password.

To require authentication see [Authentication](../../security/authentication.md#static-credentials).

Authentication is always enabled when using the [Kafka API in Yandex Cloud](https://yandex.cloud/en/docs/data-streams/kafkaapi/auth).

## How does authentication work in the Kafka API?

The Kafka API uses the `SASL_PLAINTEXT/PLAIN`, `SASL_SSL/PLAIN`and mTLS authentication mechanisms.

### Auhentication using PLAIN or SCRAM-SHA-256

The following variables are required for authentication:

* `<user-name>` — the username. For information about user management, refer to the [Authorization](../../security/authorization.md#user) section.
* `<password>` — the user's password. For information about user management, refer to the [Authorization](../../security/authorization.md#user) section.
* `<database>` — [the database path](../../concepts/connect.md#database).

These parameters form the following variables, which you can use in the `sasl.jaas.config` Kafka client property:

* `<sasl.username> = <user-name>@<database>`
* `<sasl.password> = <password>`

{% note info %}

The `<sasl.username>` and `<sasl.password>` parameters are formed differently. See [examples](./examples#authentication-in-cloud-examples) for details.

{% endnote %}

For authentication examples, see [Kafka API usage examples](./examples.md).

### Authentication using mTLS
To enable mTLS authentication, the following steps are required.

#### Server and client certificates creation

For each step below the examples of commands are given. Substitute \*\*\* with your values.

1. Create Certificate Authority (CA)

```bash
openssl genrsa -out ca-key.pem 4096
```

```bash
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem -subj "/C=***/ST=***/L=***/O=***/CN=MyKafkaRootCA"
```

2. Create server certificate

```bash
openssl genrsa -out server-key.pem 4096
```

In the next command put your host name instead of `serverhost.com`.

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

3. Create client certificate

```bash
openssl genrsa -out client-key.pem 4096
```

Substitute `clienthost.com` with hostname of your client.

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


4. Add created certificates to keystore and truststore

For server:

```bash
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem -out server.p12 -name kafka-server -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore server.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-server 

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore server.truststore.jks -storepass changeit -noprompt
```

For client:

```bash
openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -out client.p12 -name kafka-client -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-client  

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore client.truststore.jks -storepass changeit -noprompt  
```

After fulfilling these steps you should obtain keystore and truststore, as well as files with certificates and keys.

#### Client configuration
##### Java SDK example
```java
props.put("security.protocol", "SSL");
props.put("ssl.truststore.password", "changeit");
props.put("ssl.truststore.location", "/full/path/to/client.truststore.jks");
props.put("ssl.keystore.location", "/full/path/to/client.keystore.jks");
props.put("ssl.keystore.password", "changeit");
props.put("ssl.key.password", "changeit");
props.put("ssl.endpoint.identification.algorithm", "");
```

##### Kafka cli example
```
security.protocol=SSL
ssl.truststore.password=changeit
ssl.truststore.location=/full/path/to/client.truststore.jks
ssl.keystore.location=/full/path/to/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.endpoint.identification.algorithm=
```

#### YDB configuration

Firstly, it is necessary to specify the required fields in the kafka_proxy configuration.

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

In the client_certificate_authorization configuration, specify the following authentication rules:

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

Learn more about the client_certificate_authorization configuration: [{#T}](../configuration/client_certificate_authorization.md)

Additionally, for proper operation, you need to specify the path to the same server certificate for the grpc configuration:
```yaml
grpc_config:
  cert: "/path/to/server-cert.pem"
```

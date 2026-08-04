# Kafka API authentication

## Enabling authentication

When [self-deploying YDB](../../quickstart.md), [anonymous authentication](../../security/authentication.md#anonymous) is used by default, which does not require a login and password.

To enable mandatory authentication, follow the instructions in the [Authentication](../../security/authentication.md#static-credentials) article.

Authentication is always enabled when using [Kafka API in Yandex Cloud](https://yandex.cloud/en/docs/data-streams/kafkaapi/auth).

## Authentication mechanisms

Kafka API supports two SASL authentication mechanisms: `PLAIN` and `SCRAM-SHA-256`, as well as mTLS authentication.

### Authentication using PLAIN and SCRAM-SHA-256

Both mechanisms can be performed both inside the `TLS` protocol and outside, resulting in the following combinations:

* `SASL_PLAINTEXT/PLAIN`
* `SASL_SSL/PLAIN`
* `SASL_PLAINTEXT/SCRAM-SHA-256`
* `SASL_SSL/SCRAM-SHA-256`

{% note warning %}

Using the `SCRAM-SHA-256` mechanism for authenticating existing users may require a password change.

{% endnote %}

For authentication, you need:

* `<user-name>` — username. For user management, see the [{#T}](../../security/authentication.md) section.
* `<password>` — user password. For user management, see the [{#T}](../../security/authentication.md) section.
* `<database>` — path to the [database](../../concepts/connect#database) with which further interaction is intended. (Required only for the `PLAIN` mechanism).

For the `SCRAM-SHA-256` mechanism, the database is determined based on the [Kafka Connect](./connect/connect-step-by-step.md) connection settings.
The target database is the one to which the [database node](../../concepts/glossary#database-node) with the specified `<ydb-endpoint>` belongs.

From these parameters, the following variables are formed, which you can use in the
`sasl.jaas.config` parameter of the Kafka client configuration:

* `<sasl.username>` = `<user-name>[@<database>]`
* `<sasl.password>` = `<password>`

{% note warning %}

Note that the logic for forming `<sasl.username>` and `<sasl.password>` in cloud installations of {{ ydb-short-name }} may differ from what is described here.

{% endnote %}

For authentication examples, see [Reading and Writing](./examples.md).

### mTLS authentication {#device-auth}

To allow a Kafka client to authenticate devices using mTLS, follow these steps.

#### Creating server and client certificates

For each step below, example commands are provided. Replace *** with your values.

1. Create a Certificate Authority (CA)


```bash
openssl genrsa -out ca-key.pem 4096
```


```bash
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem -subj "/C=***/ST=***/L=***/O=***/CN=MyKafkaRootCA"
```


2. Create a server certificate


```bash
openssl genrsa -out server-key.pem 4096
```


In the following command, also replace `serverhost.com` with your hostname.


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


3. Create a client certificate


```bash
openssl genrsa -out client-key.pem 4096
```


Replace `clienthost.com` with the hostname of your client.


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


4. Add the certificates to the keystore and truststore

For the server:


```bash
openssl pkcs12 -export -in server-cert.pem -inkey server-key.pem -out server.p12 -name kafka-server -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore server.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-server 

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore server.truststore.jks -storepass changeit -noprompt
```


For the client:


```bash
openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -out client.p12 -name kafka-client -CAfile ca-cert.pem -caname root -password pass:changeit

keytool -importkeystore -deststorepass changeit -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass changeit -alias kafka-client  

keytool -import -trustcacerts -alias ca -file ca-cert.pem -keystore client.truststore.jks -storepass changeit -noprompt  
```


After these steps, you should have the required keystore and truststore, as well as certificate and key files.

#### Client configuration

##### Example for Java SDK


```java
props.put("security.protocol", "SSL");
props.put("ssl.truststore.password", "changeit");
props.put("ssl.truststore.location", "/full/path/to/client.truststore.jks");
props.put("ssl.keystore.location", "/full/path/to/client.keystore.jks");
props.put("ssl.keystore.password", "changeit");
props.put("ssl.key.password", "changeit");
props.put("ssl.endpoint.identification.algorithm", "");
```


##### Example for Kafka CLI


```text
security.protocol=SSL
ssl.truststore.password=changeit
ssl.truststore.location=/full/path/to/client.truststore.jks
ssl.keystore.location=/full/path/to/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.endpoint.identification.algorithm=
```


#### YDB configuration

You need to specify the required fields in the [kafka_proxy_config](../configuration/kafka_proxy_config.md) configuration.


```yaml
kafka_proxy_config:
  enable_kafka_proxy: true
  listening_port: your_port

  mtls_enable: true
  key: "server-key.pem" # specify the correct paths to files
  cert: "server-cert.pem"
  ca: "ca-cert.pem"
  enable_self_signed_certs: true # do you allow self-signed certificates
```


Also specify in the [client_certificate_authorization](../configuration/client_certificate_authorization.md) configuration the rules by which authentication will be performed:


```yaml
client_certificate_authorization:
  client_certificate_definitions:
    - require_same_issuer: true
      subject_terms:
        - short_name: CN
          suffixes:
            - '.myhost.net' # need to replace with the required suffix
      member_groups:
        - user@cert # replace with the required member group
  request_client_certificate: true
```


For correct operation, you must use the same certificate as in the gRPC settings, so you need to specify the path to the same server certificate in the gRPC configuration.

Currently, it is not possible to configure Kafka and gRPC with different server certificates, or to specify the server certificate only in the kafka_proxy_config settings when using mTLS.


```yaml
grpc_config:
  cert: "/path/to/server-cert.pem"
```

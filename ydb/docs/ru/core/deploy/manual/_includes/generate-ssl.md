## Создайте TLS сертификаты с использованием OpenSSL {# generate-tls}
{% note info %}

Вы можете использовать существующие TLS сертификаты. Важно, чтобы сертификаты поддерживали как серверную, так и клиентскую аутентификацию (`extendedKeyUsage = serverAuth,clientAuth`)

{% endnote %}

### Создайте CA ключ и сертификат {# generate-ca}
Создайте директории `secure`, в которой будет храниться ключ CA, и `certs` для сертификатов и ключей нод:
```bash
mkdir secure
mkdir certs
```

Создайте конфигурационный файл `ca.cnf` со следующим содержимым
```text
[ ca ]
default_ca = CA_default

[ CA_default ]
default_days = 365
database = index.txt
serial = serial.txt
default_md = sha256
copy_extensions = copy
unique_subject = no

[ req ]
prompt=no
distinguished_name = distinguished_name
x509_extensions = extensions

[ distinguished_name ]
organizationName = YDB
commonName = YDB CA

[ extensions ]
keyUsage = critical,digitalSignature,nonRepudiation,keyEncipherment,keyCertSign
basicConstraints = critical,CA:true,pathlen:1

[ signing_policy ]
organizationName = supplied
commonName = optional

[ signing_node_req ]
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth

# Used to sign client certificates.
[ signing_client_req ]
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth
```

Создайте CA ключ, выполнив команду
```bash
openssl genrsa -out secure/ca.key 2048
```
 Сохраните этот ключ отдельно, он необходим для выписывания сертификатов. При его утере вам необходимо будет перевыпустить все сертификаты.

 Создайте частный Certificate Authority (CA) сертификат, выполнив команду
```bash
openssl req -new -x509 -config ca.cnf -key secure/ca.key -out ca.crt -days 365 -batch
```

### Создание ключей и сертификатов для нод кластера {# generate-node-keypair}
Создайте конфигурационный файл `node.conf` со следующим содержимым:
```text
# OpenSSL node configuration file
[ req ]
prompt=no
distinguished_name = distinguished_name
req_extensions = extensions

[ distinguished_name ]
organizationName = YDB

[ extensions ]
subjectAltName = DNS:<node>.<domain>
```

Создайте ключ сертификата следующей командой:
```bash
openssl genrsa -out node.key 2048
```

Создайте Certificate Signing Request (CSR) следующей командой:
```bash
openssl req -new -sha256 -config node.cnf -key certs/node.key -out node.csr -batch
```

Создайте сертификат ноды следующей командой:
```bash
openssl ca -config ca.cnf -keyfile secure/ca.key -cert certs/ca.crt -policy signing_policy \
-extensions signing_node_req -out certs/node.crt -outdir certs/ -in node.csr -batch
```

Создайте аналогичные пары сертификат-ключ для каждой ноды.
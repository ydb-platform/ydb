## Create TLS certificates using OpenSSL {# generate-tls}

{% note info %}

You can use existing TLS certificates. It's important that certificates support both server and client authentication (`extendedKeyUsage = serverAuth,clientAuth`).

{% endnote %}

### Create CA key and certificate {# generate-ca}

Create a directory named `secure` to store the CA key and one named `certs` for certificates and node keys:

```bash
mkdir secure
mkdir certs
```

Create a `ca.cnf` configuration file with the following content:

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

Create a CA key by running the command:

```bash
openssl genrsa -out secure/ca.key 2048
```

Save this key separately, you'll need it for issuing certificates. If it's lost, you'll have to reissue all certificates.

Create a private Certificate Authority (CA) certificate by running the command:

```bash
openssl req -new -x509 -config ca.cnf -key secure/ca.key -out ca.crt -days 365 -batch
```

### Creating keys and certificates for cluster nodes {# generate-node-keypair}

Create a `node.conf` configuration file with the following content:

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

Create a certificate key by running the command:

```bash
openssl genrsa -out node.key 2048
```

Create a Certificate Signing Request (CSR) by running the command:

```bash
openssl req -new -sha256 -config node.cnf -key certs/node.key -out node.csr -batch
```

Create a node certificate with the following command:

```bash
openssl ca -config ca.cnf -keyfile secure/ca.key -cert certs/ca.crt -policy signing_policy \
-extensions signing_node_req -out certs/node.crt -outdir certs/ -in node.csr -batch
```

Create similar certificate-key pairs for each node.


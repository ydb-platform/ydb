#! /usr/bin/env bash
set -e
set -x

rm *.pem
rm *.p12

# Root CA
openssl ecparam -name secp384r1 -genkey -noout -out ca-private-key.pem
openssl req -x509 -new -nodes -key ca-private-key.pem -sha512 -days 1826 -subj '/O=YDB/CN=YDB Test Root CA/emailAddress=ydb@ydb.ydb' -out root-ca.pem

# Generate EC PEM cert without password
openssl ecparam -name secp384r1 -genkey -noout -out ec-private-key.pem
openssl req -new -sha512 -nodes -key ec-private-key.pem -config cert.cfg -out ec-cert.csr
openssl x509 -req -sha512 -days 365 -in ec-cert.csr -CA root-ca.pem -CAkey ca-private-key.pem -CAcreateserial -extfile cert-ext.cfg -out ec-cert.pem

# Generate RSA PEM cert without password
openssl genrsa -out rsa-private-key.pem 2048
openssl req -new -sha512 -nodes -key rsa-private-key.pem -config cert.cfg -out rsa-cert.csr
openssl x509 -req -sha512 -days 365 -in rsa-cert.csr -CA root-ca.pem -CAkey ca-private-key.pem -CAcreateserial -extfile cert-ext.cfg -out rsa-cert.pem

# Generate EC PEM key with password
openssl ec -in ec-private-key.pem -aes256 -passout 'pass:p@ssw0rd' -out ec-private-key-pwd.pem

# Generate RSA PEM key with password
openssl rsa -in rsa-private-key.pem -aes256 -passout 'pass:p@ssw0rd' -out rsa-private-key-pwd.pem

# Generate EC PKCS#12 cert without password
openssl pkcs12 -export -in ec-cert.pem -inkey ec-private-key.pem -name 'Test cert' -passout 'pass:' -out ec-cert.p12

# Generate RSA PKCS#12 cert without password
openssl pkcs12 -export -in rsa-cert.pem -inkey rsa-private-key.pem -name 'Test cert' -passout 'pass:' -out rsa-cert.p12

# Generate EC PKCS#12 cert with password
openssl pkcs12 -export -in ec-cert.pem -inkey ec-private-key.pem -name 'Test cert' -passout 'pass:p@ssw0rd' -out ec-cert-pwd.p12

# Generate RSA PKCS#12 cert with password
openssl pkcs12 -export -in rsa-cert.pem -inkey rsa-private-key.pem -name 'Test cert' -passout 'pass:p@ssw0rd' -out rsa-cert-pwd.p12

# Concat EC PEM cert and private key into one file
cat ec-cert.pem ec-private-key.pem > ec-all.pem
cat ec-private-key-pwd.pem ec-cert.pem > ec-all-pwd.pem

# Concat RSA PEM cert and private key into one file
cat rsa-cert.pem rsa-private-key.pem > rsa-all.pem
cat rsa-private-key-pwd.pem rsa-cert.pem > rsa-all-pwd.pem

# Cleanup
rm *.csr

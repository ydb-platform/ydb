#!/bin/sh

set -ex

# Clean keys for previous run
rm ca.key ca.crt \
   server.key server.csr server.crt server.pem server.ext \
   client.key client.csr client.crt client.pem || true


HOSTNAME=$(hostname -f)
SUBJECT="/C=RU/L=Moscow/O=yq-connector/OU=yandex/CN=${HOSTNAME}/emailAddress=vitalyisaev@yandex-team.ru"
SUBJECT_ALT_NAME="subjectAltName = DNS:${HOSTNAME},DNS:localhost"

# Generate self signed root CA cert
openssl req -nodes -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -subj "${SUBJECT}"

# Server key pair
openssl req -newkey rsa:2048 -nodes -keyout server.key -subj "${SUBJECT}" -out server.csr
echo -n "${SUBJECT_ALT_NAME}" > server.ext
openssl x509 -req -extfile server.ext -days 365 -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
cat server.key server.crt > server.pem

# Client key pair
openssl req -nodes -newkey rsa:2048 -keyout client.key -out client.csr -subj "${SUBJECT}" -addext "${SUBJECT_ALT_NAME}"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAserial ca.srl -out client.crt
cat client.key client.crt > client.pem

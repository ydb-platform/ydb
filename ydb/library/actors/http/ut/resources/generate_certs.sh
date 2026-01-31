#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"

echo "Generating CA certificate and key..."
openssl genrsa -out ca.key 2048 > /dev/null 2>&1
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/CN=Test CA/O=YDB Test/C=RU" > /dev/null 2>&1

echo "Generating server certificate and key..."
openssl genrsa -out server.key 2048 > /dev/null 2>&1
openssl req -new -key server.key -out server.csr -subj "/CN=test-server/O=YDB Test/C=RU" > /dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -set_serial 0x01 -out server.crt -days 3650 -sha256 > /dev/null 2>&1

echo "Generating valid client certificate and key..."
openssl genrsa -out client.key 2048 > /dev/null 2>&1
openssl req -new -key client.key -out client.csr -subj "/CN=test-client/O=YDB Test/C=RU" > /dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -set_serial 0x02 -out client.crt -days 3650 -sha256 > /dev/null 2>&1

echo "Generating untrusted CA certificate and key (different CA for untrusted client cert)..."
openssl genrsa -out untrusted_ca.key 2048 > /dev/null 2>&1
openssl req -x509 -new -nodes -key untrusted_ca.key -sha256 -days 3650 -out untrusted_ca.crt -subj "/CN=Untrusted Test CA/O=YDB Test/C=RU" > /dev/null 2>&1

echo "Generating untrusted client certificate and key (signed by different CA)..."
openssl genrsa -out untrusted_client.key 2048 > /dev/null 2>&1
openssl req -new -key untrusted_client.key -out untrusted_client.csr -subj "/CN=untrusted-client/O=YDB Test/C=RU" > /dev/null 2>&1
openssl x509 -req -in untrusted_client.csr -CA untrusted_ca.crt -CAkey untrusted_ca.key -set_serial 0x01 -out untrusted_client.crt -days 3650 -sha256 > /dev/null 2>&1

echo "Cleaning up intermediate files..."
rm -f *.csr *.srl

echo "Successfully generated certificate files in resources/:"
echo "  - ca.crt (CA certificate)"
echo "  - server.crt, server.key (server certificate and key)"
echo "  - client.crt, client.key (client certificate and key)"
echo "  - untrusted_ca.crt (untrusted CA certificate)"
echo "  - untrusted_client.crt, untrusted_client.key (untrusted client certificate and key)"
echo "Certificates are valid for 10 years (3650 days)"

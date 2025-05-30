openssl req -x509 -noenc -newkey rsa:4096 -keyout ca_key.pem -out ca.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=Localhost CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=keyCertSign"

openssl req -new -x509 -noenc -newkey rsa:4096 -keyout key.pem -out cert.pem \
    -CA ca.pem -CAkey ca_key.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=localhost" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth,clientAuth"

openssl req -x509 -noenc -newkey rsa:4096 -keyout ca_key_with_ip_in_san.pem -out ca_with_ip_in_san.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=Localhost CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=keyCertSign"

openssl req -new -x509 -noenc -newkey rsa:4096 -keyout key_with_ip_in_san.pem -out cert_with_ip_in_san.pem \
    -CA ca_with_ip_in_san.pem -CAkey ca_key_with_ip_in_san.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth,clientAuth"

openssl req -x509 -noenc -newkey ec:<(openssl ecparam -name prime256v1) -keyout ca_key_ec.pem -out ca_ec.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=Localhost CA" \
    -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage=keyCertSign"

openssl req -new -x509 -noenc -newkey ec:<(openssl ecparam -name prime256v1) -keyout key_ec.pem -out cert_ec.pem \
    -CA ca_ec.pem -CAkey ca_key_ec.pem \
    -days 3650 -subj "/C=RU/L=Moscow/O=Yandex/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth,clientAuth"

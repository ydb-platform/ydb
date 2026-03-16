pyjks
=====

<a href="https://pyjks.readthedocs.io/en/latest/"><img src="https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat"></a>
<a href="https://pypi.python.org/pypi/pyjks"><img src="https://img.shields.io/pypi/v/pyjks.svg"></a>
<a href="http://calver.org"><img src="https://img.shields.io/badge/calver-YY.MINOR.MICRO-22bfda.svg"></a>
<a href="https://github.com/kurtbrose/pyjks/blob/master/CHANGELOG.md"><img src="https://img.shields.io/badge/CHANGELOG-UPDATED-b84ad6.svg"></a>

A pure python Java KeyStore file parser, including private/secret key decryption.
Can read JKS, JCEKS, BKS and UBER (BouncyCastle) key stores.

The best way to utilize a certificate stored in a jks file up to this point has been
to use the java keytool command to transform to pkcs12, and then openssl to transform to pem.

This is better:
 -  no security concerns in passwords going into command line arguments, or unencrypted files being left around
 -  no dependency on a JVM

## Requirements:

 * Python 2.6+ or Python 3.3+
 * pyasn1 0.3.5+
 * pyasn1_modules 0.0.8+
 * javaobj-py3 0.1.4+
 * pycryptodomex, if you need to read JCEKS, BKS or UBER keystores
 * twofish, if you need to read UBER keystores

## Usage examples:

Reading a JKS or JCEKS keystore and dumping out its contents in the PEM format:
```python
from __future__ import print_function
import sys, base64, textwrap
import jks

def print_pem(der_bytes, type):
    print("-----BEGIN %s-----" % type)
    print("\r\n".join(textwrap.wrap(base64.b64encode(der_bytes).decode('ascii'), 64)))
    print("-----END %s-----" % type)

ks = jks.KeyStore.load("keystore.jks", "XXXXXXXX")
# if any of the keys in the store use a password that is not the same as the store password:
# ks.entries["key1"].decrypt("key_password")

for alias, pk in ks.private_keys.items():
    print("Private key: %s" % pk.alias)
    if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
        print_pem(pk.pkey, "RSA PRIVATE KEY")
    else:
        print_pem(pk.pkey_pkcs8, "PRIVATE KEY")

    for c in pk.cert_chain:
        print_pem(c[1], "CERTIFICATE")
    print()

for alias, c in ks.certs.items():
    print("Certificate: %s" % c.alias)
    print_pem(c.cert, "CERTIFICATE")
    print()

for alias, sk in ks.secret_keys.items():
    print("Secret key: %s" % sk.alias)
    print("  Algorithm: %s" % sk.algorithm)
    print("  Key size: %d bits" % sk.key_size)
    print("  Key: %s" % "".join("{:02x}".format(b) for b in bytearray(sk.key)))
	print()
```


Transforming an encrypted JKS/JCEKS file into an OpenSSL context:
```python
import OpenSSL
import jks

_ASN1 = OpenSSL.crypto.FILETYPE_ASN1

def jksfile2context(jks_file, passphrase, key_alias, key_password=None):
    keystore = jks.KeyStore.load(jks_file, passphrase)
    pk_entry = keystore.private_keys[key_alias]
    # if the key could not be decrypted using the store password, decrypt with a custom password now
    if not pk_entry.is_decrypted():
        pk_entry.decrypt(key_password)

    pkey = OpenSSL.crypto.load_privatekey(_ASN1, pk_entry.pkey)
    public_cert = OpenSSL.crypto.load_certificate(_ASN1, pk_entry.cert_chain[0][1])
    trusted_certs = [OpenSSL.crypto.load_certificate(_ASN1, cert.cert) for alias, cert in keystore.certs]

    ctx = OpenSSL.SSL.Context(OpenSSL.SSL.TLSv1_METHOD)
    ctx.use_privatekey(pkey)
    ctx.use_certificate(public_cert)
    ctx.check_privatekey() # want to know ASAP if there is a problem
    cert_store = ctx.get_cert_store()
    for cert in trusted_certs:
        cert_store.add_cert(cert)
    return ctx

```

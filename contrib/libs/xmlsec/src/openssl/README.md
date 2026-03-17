# XMLSec Library: XMLSEC-OPENSSL

## What version of OpenSSL?
OpenSSL 1.1.1 or later is required. Note that support for  OpenSSL 1.0.0 and
OpenSSL 1.1.0 is deprecated and will be removed in the future releases.

## Keys manager
OpenSSL does not have a keys or certificates storage implementation. The
default xmlsec-openssl key manager uses XMLSEC Simple Keys Store based on
a plain keys list. Trusted/untrusted certificates are stored in `STACK_OF(X509)`
structures.

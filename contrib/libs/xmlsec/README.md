# XMLSec Library

XMLSec library provides C based implementation for major XML Security
standards:
- [XML Signature Syntax and Processing](https://www.w3.org/TR/xmldsig-core)
- [XML Encryption Syntax and Processing](https://www.w3.org/TR/xmlenc-core/)

## Documentation
Complete XMLSec library documentation is published on [XMLSec website](https://www.aleksey.com/xmlsec/).

## License
XMLSec library is released under the MIT Licence (see the [Copyright file](Copyright).

## Building and installing XMLSec

### Prerequisites
XMLSec requires the following libraries:
- [LibXML2](http://xmlsoft.org)
- [LibXSLT](http://xmlsoft.org/XSLT/)

And at least one of the following cryptographic libraries:
- [OpenSSL](http://www.openssl.org)
- [NSS](https://firefox-source-docs.mozilla.org/security/nss/index.html)
- [GCrypt/GnuTLS](https://www.gnutls.org/)
- MS Crypto API (Windows only)
- MS Crypto API NG (Windows only)

For example, the following packages need to be installed on Ubuntu to build
XMLSec library:
```
  # common build tools
  apt install automake autoconf libtool libtool-bin gcc

  # ltdl is required to support dynamic crypto libs loading
  apt install libltdl7 libltdl-dev

  # core libxml2 and libxslt libraries
  apt install libxml2 libxml2-dev libxslt1.1 libxslt1-dev

  # openssl libraries
  apt install libssl1.1 libssl-dev

  # nspr/nss libraries
  apt install libnspr4 libnspr4-dev libnss3 libnss3-dev libnss3-tools

  # gcrypt/gnutls libraries
  apt install libgcrypt20 libgcrypt20-dev libgnutls28-dev

  # required for building man pages and docs
  apt install help2man man2html gtk-doc-tools
```

### Building XMLSec on Linux, Unix, MacOSX, MinGW, Cygwin, etc

To build and install XMLSec library on Unix-like systems run the following commands:

```
  gunzip -c xmlsec1-xxx.tar.gz | tar xvf -
  cd xmlsec1-xxxx
  ./configure [possible options]
  make
  make check
  make install
```

To see the configuration options, run:

```
  ./configure --help
```


### Building XMLSec on Windows

See [win32/README.md](win32/README.md) for details.

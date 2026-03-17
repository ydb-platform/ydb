# Python software webauthn token

[![Build Status](https://travis-ci.org/bodik/soft-webauthn.svg?branch=master)](https://travis-ci.org/bodik/soft-webauthn)

Package is used for testing webauthn enabled web applications. The use-case is
authenticator and browser emulation during web application development
continuous integration.

`SoftWebauthnDevice` class interface exports basic navigator interface used for
webauthn features:

* `SoftWebauthnDevice.create(...)` aka `navigator.credentials.create(...)`
* `SoftWebauthnDevice.get(...)` aka `navigator.credentials.get(...)`

To support authentication tests without prior registration/attestation, the
class exports additional functions:

* `SoftWebauthnDevice.cred_init(rp_id, user_handle)`
* `SoftWebauthnDevice.cred_as_attested()`

There is no standard/specification for *Client* (browser) to *Relying party*
(web application) communication. Therefore the class should be be used in a web
application test suite along with other code handling webapp specific tasks
such as conveying *CredentialCreationOptions* from webapp and
*PublicKeyCredential* back to the webapp.

The example usage can be found in `tests/test_interop.py` (Token/Client vs RP
API) and `tests/test_example.py` (Token/Client vs RP HTTP). Despite internal
usage of `yubico/python-fido2` package, the project should be usable againts
other RP implementations as well.

## References

* https://w3c.github.io/webauthn
* https://webauthn.guide/
* https://github.com/Yubico/python-fido2

## Development

```
git clone https://github.com/bodik/soft-webauthn
cd soft-webauthn
ln -s ../../git_hookprecommit.sh .git/hooks/pre-commit

# OPTIONAL, create and activate virtualenv
make venv
. venv/bin/activate

# install dependencies
make install-deps

# profit
make lint
make test
make coverage
```

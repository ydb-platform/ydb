# py_webauthn
[![PyPI](https://img.shields.io/pypi/v/webauthn.svg)](https://pypi.python.org/pypi/webauthn) [![GitHub license](https://img.shields.io/badge/license-BSD-blue.svg)](https://raw.githubusercontent.com/duo-labs/py_webauthn/master/LICENSE) ![Pythonic WebAuthn](https://img.shields.io/badge/Pythonic-WebAuthn-brightgreen?logo=python&logoColor=white)

A Python3 implementation of the server-side of the [WebAuthn API](https://www.w3.org/TR/webauthn-2/) focused on making it easy to leverage the power of WebAuthn.

This library supports all FIDO2-compliant authenticators, including security keys, Touch ID, Face ID, Windows Hello, Android biometrics...and pretty much everything else.

## Installation

This module is available on **PyPI**:

`pip install webauthn`

## Requirements

- Python 3.9 and up

## Usage

The library exposes just a few core methods on the root `webauthn` module:

- `generate_registration_options()`
- `verify_registration_response()`
- `generate_authentication_options()`
- `verify_authentication_response()`

Two additional helper methods are also exposed:

- `options_to_json()`
- `base64url_to_bytes()`

Additional data structures are available on `webauthn.helpers.structs`. These dataclasses are useful for constructing inputs to the methods above, and for providing type hinting to help ensure consistency in the shape of data being passed around.

Generally, the library makes the following assumptions about how a Relying Party implementing this library will interface with a webpage that will handle calling the WebAuthn API:

- JSON is the preferred data type for transmitting registration and authentication options from the server to the webpage to feed to `navigator.credentials.create()` and `navigator.credentials.get()` respectively.
- JSON is the preferred data type for transmitting WebAuthn responses from the browser to the server.
- Bytes are not directly transmittable in either direction as JSON, and so should be encoded to and decoded from [base64url to avoid introducing any more dependencies than those that are specified in the WebAuthn spec](https://www.w3.org/TR/webauthn-2/#sctn-dependencies).
  - See the [`WebAuthnBaseModel` struct](https://github.com/duo-labs/py_webauthn/blob/master/webauthn/helpers/structs.py#L13) for more information on how this is achieved

The examples mentioned below include uses of the `options_to_json()` helper (see above) to show how easily `bytes` values in registration and authentication options can be encoded to base64url for transmission to the front end.

The examples also include demonstrations of how to pass JSON-ified responses, using base64url encoding for `ArrayBuffer` values, into `parse_registration_credential_json` and `parse_authentication_credential_json` to be automatically parsed by the methods in this library. An RP can pair this with corresponding custom front end logic, or one of several frontend-specific libraries (like [@simplewebauthn/browser](https://www.npmjs.com/package/@simplewebauthn/browser), for example) to handle encoding and decoding such values to and from JSON.

Other arguments into this library's methods that are defined as `bytes` are intended to be values stored entirely on the server. Such values can more easily exist as `bytes` without needing potentially extraneous encoding and decoding into other formats. Any encoding or decoding of such values in the name of storing them between steps in a WebAuthn ceremony is left up to the RP to achieve in an implementation-specific manner.

### Registration

See **examples/registration.py** for practical examples of using `generate_registration_options()` and `verify_registration_response()`.

You can also run these examples with the following:

```sh
# See "Development" below for venv setup instructions
venv $> python -m examples.registration
```

### Authentication

See **examples/authentication.py** for practical examples of using `generate_authentication_options()` and `verify_authentication_response()`.

You can also run these examples with the following:

```sh
# See "Development" below for venv setup instructions
venv $> python -m examples.authentication
```

## Development

### Installation

Set up a virtual environment, and then install the project's requirements:

```sh
$> python3 -m venv venv
$> source venv/bin/activate
venv $> pip install -r requirements.txt
```

### Testing

Python's unittest module can be used to execute everything in the **tests/** directory:

```sh
venv $> python -m unittest
```

Auto-watching unittests can be achieved with a tool like nodemon.

**All tests:**
```sh
venv $> nodemon --exec "python -m unittest" --ext py
```

**An individual test file:**
```sh
venv $> nodemon --exec "python -m unittest tests/test_aaguid_to_string.py" --ext py
```

### Linting and Formatting

Linting is handled via `mypy`:

```sh
venv $> python -m mypy webauthn
Success: no issues found in 52 source files
```

The entire library is formatted using `black`:

```sh
venv $> python -m black webauthn --line-length=99
All done! ✨ 🍰 ✨
52 files left unchanged.
```

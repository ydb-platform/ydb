
# CHANGELOG

This file contains the list of changes made to pymonocypher.


# 4.0.2.6

2025 Dec 2

* Rebuild for Python 3.14, drop 3.10 support.
* Added linux arm64 wheel.


# 4.0.2.5

2024 Nov 11

* Rebuild for Python 3.13


## 3.1.3.4

2024 Nov 11

* Rebuild for Python 3.13


## 4.0.2.4

2024 Aug 17

* Patched GitHub action based upon 3.1.3.3 release.
* Removed unnecessary numpy dependency.
* Fixed argon2i_32
  * Changed incorrect nb_block to nb_blocks
  * Added "_wipe" argument to optionally skip wipe operation.
  * Added unit test


## 3.1.3.3

2024 June 5

* Rebuild for Python 3.12


## 4.0.2.3

2024 May 29

* Restore compute_signing_public_key 32-byte variant with a deprecation warning.
* Modified signature_sign to still accept 32-byte private key.


## 4.0.2.2

2024 May 10

* Added compute_signing_public_key parameter check.
* Added argon2i_32 parameter checks.
* Added typing hints.


## 4.0.2.1

2024 May 8

* Upgraded Monocypher from 3.1.3 to 4.0.2.
* Removed incremental SignatureVerify, which was removed from Monocypher.


## 3.1.3.2

2023 Jun 7

* Migrated to GitHub Actions build & release process.
* Bumped minimum python version from 3.6 to 3.9.
* Added Elligator bindings using monocypher 4.x naming.


## 3.1.3.1

2022 Nov 30

* Fixed build dependencies.


## 3.1.3.0

2022 Oct 22

* Upgraded to Monocypher 3.1.3.
* Removed print generated keys to stdout.


## 3.1.0.0

2020 May 10

*   Upgraded to Monocypher 3.1.0.
*   Removed deprecated incremental encryption interface.
*   Changed versioning to match monocypher.
*   Added package metadata.


## 0.1.4

2019 Oct 22

*   Upgraded to Monocypher 2.0.6.


## 0.1.3

2019 Sep 28

*   Distinguished between public keys for signing and key exchange.
    Use new functions generate_signing_key_pair and compute_signing_public_key
    for signing.  Use new functions compute_key_exchange_public_key and
    generate_key_exchange_key_pair for key exchange.
*   Deprecated generate_key_pair and public_key_compute which
    call generate_signing_key_pair and compute_signing_public_key,
    respectively.

Thank you to wamserma and yota-code!


## 0.1.2

2018 Oct 10

*   Fixed setup.py typo that prevented installation.


## 0.1.1

2018 Oct 9

*   Initial public release based upon Monocypher 2.0.5.

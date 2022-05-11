# PKCS#11 tests

To run the PKCS#11 tests, configure cmake with: `-DENABLE_PKCS11_TESTS=ON`

and set the following environment variables:

```
TEST_PKCS11_LIB = <path-to-shared-lib>
TEST_PKCS11_TOKEN_DIR = <path-to-softhsm-token-dir>
```
TEST_PKCS11_LIB is used by the tests to peform pkcs11 operations.

TEST_PKCS11_TOKEN_DIR is used by the tests to clear the softhsm tokens before a test begins. This is achieved by cleaning the token directory <b>NOTE: Any tokens created outside the tests will be cleaned up along with all the objects/keys on it as part of the tests.</b> 


## The suggested way to set up your machine
1)  Install [SoftHSM2](https://www.opendnssec.org/softhsm/) via brew / apt / apt-get / yum:
    ```
    > apt install softhsm
    ```

    Check that it's working:
    ```
    > softhsm2-util --show-slots
    ```

    If this spits out an error message, create a config file:
    *   Default location: `~/.config/softhsm2/softhsm2.conf`
    *   This file must specify token dir, default value is:
        ```
        directories.tokendir = /usr/local/var/lib/softhsm/tokens/
        ```

2)  Set env vars like so:
    ```
    TEST_PKCS11_LIB = <path to libsofthsm2.so>
    TEST_PKCS11_TOKEN_DIR = /usr/local/var/lib/softhsm/tokens/
    ```


3)  [Example to import your keys, Not used by tests] Create token and private key

    You can use any values for the labels, pin, key, cert, CA etc.
    Here are copy-paste friendly commands for using files available in this repo.
    ```
    > softhsm2-util --init-token --free --label my-test-token --pin 0000 --so-pin 0000
    ```

    Note which slot the token ended up in

    ```
    > softhsm2-util --import tests/resources/unittests.p8 --slot <slot-with-token> --label my-test-key --id BEEFCAFE --pin 0000
    ```
    <b>WARN: All tokens created outside the tests would be cleaned up as part of the tests, Use a separate token directory for running the tests if you would like to keep your tokens intact.</b>


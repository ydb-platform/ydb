IBAN registry
=============

The IBAN registry contains data used for validation of the country specific IBAN format.  All files
within this directory are sourced in alpha-numeric sorting order on package import and should have
the following format

  ```json
  {
    "AD": {
      "bban_spec": "4!n4!n12!c",
      "iban_spec": "AD2!n4!n4!n12!c",
      "bban_length": 20,
      "iban_length": 24,
      "positions": {
        "account_code": [
          8,
          20
        ],
        "bank_code": [
          0,
          4
        ],
        "branch_code": [
          4,
          8
        ]
      }
    }
  }
  ```

This information is currently derived from the offical SWIFT IBAN specification and it should not be
necessary to overwrite this data.

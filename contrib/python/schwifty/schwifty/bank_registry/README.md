Bank registry
=============

This registry holds information about banks that is used to map the name of a bank to a BIC and to
allow to create a BIC from a domestic bank code. Each JSON file within this directory is
automatically read on import of the schwifty package and should contain a list in the following
format. 

  ```json
  [
    {
      "bank_code": "10000000",
      "name": "Bundesbank",
      "short_name": "BBk Berlin",
      "bic": "MARKDEF1100",
      "primary": true,
      "country_code": "DE"
    }
  ]
  ```

The files are sourced in alphanumeric sorting order. Generated files, e.g. those that are derived
from some officially provided data-source, should be prefixed with `generated_`. Files that contain
manually curated data should be prefixed with `manual_`. 
